# Workflow streaming implementation review — fifth round

**This is a review artifact, not part of the design.** Like `review-guide.md`, `follow-up-review.md`,
`third-review.md` and `fourth-review.md`, it records what was found and what was done about it.
Everything else in this directory states current truth and carries no revision narrative; the specs
and decision records named below were updated in place, and those are the authority on what the code
now does.

Reviewed revisions: `sdk-python` `065e7f5b` for findings 1-4, `5ab10ac5` for 5-8, and the working
tree of the fix for 7 for finding 9 · `sdk-rust` `7e9d8ac8`

Nine defects, all Python-side, all P1, in three passes: four against the fourth round's fixes, four
more against the fixes for those, and one against the fix for finding 7. Every one was found by
static reading and reported as such — **none was reproduced before it was reported**, which is a
weaker bar than the fourth round's and is the reason each entry below names the test that now holds
it. Each fix is covered by a test confirmed to fail against the pre-fix code by reverting the fix and
re-running it, which is step 4 of "before reporting a defect found by a test" in
`verification-hazards.md`.

| # | Finding | Fixed in |
|---|---|---|
| 1 | Continue-As-New restores a cursor into the wrong backend | `5ab10ac5` |
| 2 | Continue-As-New snapshots consumption before the activation is finished | `5ab10ac5` |
| 3 | A write fence can overtake a preceding concurrent publish | `5ab10ac5` |
| 4 | Offline replay converts one record under two serialization contexts | `5ab10ac5` |
| 5 | A failed fence is misclassified as a failed data write | this round |
| 6 | A conflicted append resolution can release a false fence | this round |
| 7 | Continuation schema version 2 is not safe in a mixed-version Worker fleet | this round |
| 8 | Provider format version zero bypasses continuation validation | this round |
| 9 | Old Workers ignore the binding extension and restore a foreign cursor | this round |

Findings 5, 6 and 8 are in the code that closed 1 and 3, 7 is in the wire format that closed 1, and 9
is in the fix for 7. That is the fourth round's shape once more — a guarantee written for the path it
was aimed at and not for the adjacent one — with two additions worth naming on their own.

**Three of the four second-pass findings are a state machine with two states where the domain has
three.** A ledger holding two kinds of entry that reads them as one kind. An unknown append outcome
that resolves two ways, distinguished by a dictionary entry both ways remove. A version integer whose
"absent" and "zero" were the same test.

**Finding 9 is the round's one wrong *fix* rather than one wrong line**, and the only finding here
that argued a fix had chosen the wrong direction outright. Findings 7 and 9 are in tension by
construction — 7 says an old Worker must not be stalled by a header it cannot read, 9 says it must
not be allowed to restore a cursor it cannot check — and the fix for 7 resolved that tension in the
direction this feature's own taxonomy forbids. See ADR-039 for where the line now falls and why.

## 1 — Continue-As-New restores a cursor into the wrong backend

A `Continuation` held cursors and stream names and nothing about the store. A Worker can keep a
backend name while mapping it to another implementation, so a successor could hand a predecessor's
offset to a backend that never held those records — accepted, and skipping everything before that
boundary in silence. Marker replay makes this comparison for every recorded range it reads; a
successor's first live read precedes any marker that could make it.

The continuation now carries the whole binding, and restoration compares it before the cursor reaches
the manager: a changed stream or backend name is a nondeterminism error, matching `_verify_binding()`,
and a changed provider identity or format version is a storage failure, matching the check marker
replay already performs. `spec/annotation-format.md`, `spec/failure-taxonomy.md`, ADR-039.

## 2 — Continue-As-New snapshots consumption before the activation is finished

The header was serialised while the Continue-As-New command was created, but the Workflow's event
loop keeps draining ready tasks after a terminal command is added. A consumer that ran later advanced
the consumption cursor past the recorded boundary — and *did* reach the predecessor's final marker,
which is closed on the way out — so the successor was handed that record a second time.

The header is now re-serialised where the activation emits its stream commands, the first point at
which no user code can still run and the same place the observation delta that commits the boundary
is emitted. Neither this nor finding 1's content change needs an internal flag: Core matches a
Continue-As-New command to its history event by command type alone and never compares the recorded
header. `spec/annotation-format.md`.

## 3 — A write fence can overtake a preceding concurrent publish

`publish()` draws its sequence before awaiting the payload codec, on purpose, so a publish invoked
first can still be encoding when `finish_writing()` is called — invisible to the backend and to the
unresolved-append check alike. The fence's claim was therefore the one thing the fence did not check.
With `wake=False` batching it also spent the batch's only wake before the data existed.

A per-`StreamKey` append order on the producer, shared by every handle for that key, now holds a
fence until each earlier call on the stream has settled. `spec/backend-contract.md`, ADR-040.

## 4 — Offline replay converts one record under two serialization contexts

The manager bound the asynchronous half of decoding to the namespace recorded in the annotation,
which is correct — the Workflow has not run far enough to have re-created the subscription — while
the runtime's `codec_for()` bound the synchronous half to the Worker's own namespace. Those are the
same identity on a live Worker and are not the same under an offline `Replayer`, which runs under a
placeholder namespace by default, so a context-sensitive converter failed or converted differently on
a valid history.

The replay plan's recorded bindings now produce a context-bound converter per wait, built outside the
sandbox because `with_context` runs user cloning code, and `codec_for()` selects it by wait ID. They
are kept for the Run rather than cleared when replay ends, since a drained replay batch may be
consumed a prefix at a time. `spec/python-runtime.md`.

## 5 — A failed fence is misclassified as a failed data write

Finding 3's coordinator put publishes and fences in one order, and a fence waited on everything
unsettled when it began — including earlier fences. An earlier fence that never reached the backend,
cancelled while waiting or refused before appending, is not a data write that went missing, but it
presented as one: a later fence with every write it claims already durable was refused with
`PrecedingWriteFailedError`, an error whose public contract is that an earlier `publish()` failed.

Fences now read the order without joining it. Two concurrent fences make independent claims about the
publishes each was invoked after, so neither has to wait for the other. ADR-040 records why the
alternative — keeping both kinds in the ledger and tagging them — was not taken.

## 6 — A conflicted append resolution can release a false fence

An append with no answer is *unknown*, and the fence waits rather than refusing. What ends the wait is
`resolve_append()`, which ends it two ways: the record is durable, or the backend refuses the key and
the record demonstrably never landed. The fix for 3 decided between them by whether the record was
still in the producer's unresolved set — and **both outcomes remove it**. A conflict therefore read as
durability, and the fence appended over the hole while claiming the batch complete.

`AppendConflictError` is not an exotic backend violation; it is the one definite refusal the contract
defines, reachable from a session-id collision or a retry that reused a sequence for other bytes.

An unresolved append now carries the operation whose outcome it decides, and `resolve_append()`
reports the definitive answer to it — cleared on a durable record, replaced by the conflict on a
refusal. That exposed a second window in the same code: the fence read each outcome as it settled, so
a resolution that landed while the fence was still waiting on a *later* publish was missed. It now
reads every outcome after its last wait. `spec/backend-contract.md`, ADR-040.

**Two of this fix's own guarantees were initially unasserted**, and mutation testing is what found
them: deleting the line that reports a *durable* resolution changed no test result, and so did
capturing each earlier outcome at wait time instead of re-reading it. The first gap was a missing
case — a recovery that succeeds and must release the fence rather than refuse it. The second was a
missing *schedule*: every test had a single preceding write, so the resolution always landed before
the fence's next turn, and the window the second pass actually closed — a resolution arriving while
the fence sits on a **later** publish — needed two writes to reach. Both are covered now, and both
mutations fail a test. This is step 4 of `verification-hazards.md` applied to a fix rather than to a
defect report, and it is worth doing on any fix whose statement is about ordering.

## 7 — Continuation schema version 2 is not safe in a mixed-version Worker fleet

Finding 1's fix bumped the header's schema version and wrote the new version unconditionally, with
old Workers refusing it. But this header is written by one Run and read by the **next Run's** Worker,
which on an unversioned task queue can be an older one, and it is read while the successor's runtime
is built — before any Workflow code runs. So a rolling upgrade stalled the successor on its first
Workflow Task, every retry failed identically, and a rollback made it permanent.

The comment justifying the unflagged bump was about Core not comparing Continue-As-New headers. That
is true and it covers replay of the *predecessor*; it says nothing about the copy the server persists
in the successor's `WorkflowExecutionStarted`.

The first fix wrote the version 1 envelope with the binding appended behind the entries a version 1
reader consumes, so nothing had to be arranged in either direction. Finding 9 is that fix.

## 9 — Old Workers ignore the binding extension and restore a foreign cursor

The extension bought **syntactic** compatibility and nothing else. An old Worker that parses the
envelope does not thereby acquire the check: it runs the restoration it shipped with, which compares
the stream name alone, and accepts a cursor it cannot vouch for. Where its registry resolves that
backend name to another store, it hands the cursor over and skips every record below the boundary —
finding 1 exactly, reintroduced in precisely the mixed-fleet situation the extension existed to
support, and silently where the version bump was loud.

**No arrangement of bytes can make deployed code perform a check it has no code for.** So the binding
is must-understand data and the schema version is what enforces it: a live Continue-As-New writes a
version an unaware Worker refuses, and the binding is encoded inline in each entry rather than
appended after them, because a trailing block is exactly what an unknowing reader steps over.

Finding 7's hazard is not thereby dismissed — it is accepted, and its cost stated: a stalled
successor during a rolling upgrade, a blocked one under a rollback, and the deployment answer is to
stage the decoder ahead of the writer, with Worker Versioning or build routing for a fleet that
cannot tolerate the stall at all. A blocked Run is the direction ADR-014 and the failure taxonomy
take everywhere else in this feature; the extension traded an explicit retryable incompatibility for
possible silent data loss, which is that rule read backwards.

The compatibility test moved with the fix, and its old shape is worth recording as a hazard in
miniature: it asserted that an old decoder returns the cursor from a live header, and **that
successful return was the unsafe behaviour rather than evidence against it.** A test frozen at older
behaviour has to exercise that behaviour's *restoration*, not only its decoder. ADR-039.

## 8 — Provider format version zero bypasses continuation validation

`provider_format_version` is an unconstrained integer and the registry reserves no value, so zero is
a version a provider may declare and the encoding represents it exactly. Restoration used it as a
falsey "not recorded" sentinel, so a cursor written under version 0 was handed to a version 1
implementation without the mismatch being reported — making Continue-As-New less safe than marker
replay, which compares exactly, for the identical binding.

Membership decides it now. A header that recorded no version has no entry, which is also what a
header from before the binding existed decodes to, so the two need no distinguishing. Rejecting
`<= 0` at registration was the alternative and was not taken: the binary format already represents
zero, and the contract does not reserve it. ADR-039.

## What this round did not do

- **None of the nine cases is armed in the M1 gate.** They exist as tests and pass; they are not
  listed in `required-tests/tests-m1.md`, so nothing fails if one is deleted. Arming them means
  editing that list, committing in Core, and moving the vendored submodule pointer before the gate
  sees it — hazard 3 of `verification-hazards.md`.
- **No finding was reproduced before being reported.** Every fix has a test that fails without it,
  which establishes the fix, not the original report. For findings 3 and 5 through 8 the distinction
  is thin — the mechanism is a few lines of control flow — and for 2, 4 and 9 it is not: all three
  depend on scheduling or deployment shapes that a static trace can get wrong in the direction of a
  defect that is not there. All were nonetheless confirmed by the tests that now hold them, and
  finding 9's mixed-fleet claim is held by a frozen reimplementation of the older restoration rather
  than by two real Workers.
- `review-guide.md`'s change surface was refreshed to this round's heads; it remains a snapshot.
