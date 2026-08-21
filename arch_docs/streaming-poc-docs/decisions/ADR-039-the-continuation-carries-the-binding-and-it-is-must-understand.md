# ADR-039 — The continuation carries the whole binding, and it is must-understand

**Status:** Accepted · **Affects:** P15 · **Spec:** `spec/annotation-format.md`

## Context

ADR-022 settled *where* the Continue-As-New cursor travels: a reserved internal header on the
command, persisted in the successor's `WorkflowExecutionStarted`. It did not settle what travels
beside it, and the answer it shipped with was nothing — a cursor and a stream name per wait.

That is not enough to say what the cursor is a position *in*. An offset is meaningful only inside the
store that produced it, and a Worker can keep a backend name while mapping it to another
implementation, another cluster, or another format version without touching Workflow code. A
successor that resumed on the wait number alone hands a predecessor's offset to a store that never
held those records; the backend accepts it, and everything before that boundary is skipped in
silence. Marker replay compares the whole binding before it interprets a recorded range — but a
successor Run's *first live read* happens before it has written a marker that could make the
comparison.

Adding the binding to the header is therefore forced. What is not forced is what a Worker that does
not understand it should do, and that is this decision, because **this header's reader is the next
Run's Worker, not the one that wrote it.** On an unversioned task queue a Run continued by a new
Worker can have its successor picked up by an old one. The header is read while the successor's
runtime is built, before any Workflow code runs, so a Worker that cannot parse it fails that Run's
first Workflow Task — and every retry identically. A rollback to only older Workers blocks that Run
until it is rolled forward.

Core not comparing Continue-As-New headers is what makes regenerating this header safe on *replay of
the predecessor*. It says nothing about the copy the server persisted for the successor.

## Options

**A. Bump the schema version** and write the new format unconditionally. A Worker that does not know
the version refuses the header, fails the task, and retries.

**B. Bump, and gate emission on routing** — Worker Versioning or build-ID routing as a prerequisite
for writing the new version, so no old Worker can receive the successor.

**C. Bump, and gate emission on a deployment capability** staged ahead of the writer, applying across
Continue-As-New Runs.

**D. Keep the old envelope and append the binding behind the entries an old reader consumes**, so
nothing has to be deployed or routed first.

## Decision

**A**, with the *reader* for the new version staged ahead of the writer, and B available on top for a
deployment that cannot tolerate the stall.

D is the one that has to be ruled out explicitly, because it looks like it dominates A: nothing
stalls, nothing needs arranging, and old Workers keep running. It fails for a reason no byte format
can fix. **Syntactic compatibility is not semantic compatibility.** An old Worker that parses the
envelope does not thereby acquire the check — it runs the restoration it shipped with, which compares
the stream name and nothing else. So it accepts a cursor it cannot vouch for, and if its registry
resolves that backend name to another store it hands the cursor over and skips every record below the
boundary. That is exactly the failure the binding was added to prevent, reintroduced in precisely the
mixed-fleet situation D exists to support — and reintroduced *silently*, where A is loud. **A binding
is not optional metadata an old reader may ignore; it is the proof that the cursor describes the
backend about to interpret it.**

A's cost is real and is the reason B and C were considered: a stalled successor during a rolling
upgrade, and a blocked one under a rollback. But a blocked Run is the direction this feature takes
everywhere else — ADR-014 refuses to fail a Workflow for integrity loss on the grounds that a blocked
Run can be resumed after repair and a failed one cannot, and the taxonomy's standing rule is that
integrity loss must never resolve to an alternate stream result. D trades an explicit, retryable
incompatibility for possible silent data loss, which is that rule read backwards.

B is the right answer for a deployment that cannot accept the stall, and it is a deployment
mechanism rather than something the SDK can do on its own. It also has to hold for the *successor*
Run rather than for the Run that writes the header, so it is a claim about routing across a
Continue-As-New.

C is B without the enforcement. A predecessor Run's language-SDK capability flag says what the Worker
executing *that* Run can do; the failure happens in a different Run, read by whichever Worker picks
its first task up. Unless the capability also constrains or routes the successor it is not a gate,
and if it does constrain the successor it is B.

What remains of D's goal — do not strand a chain gratuitously — is met by staging the reader: the
decoder accepts the new version one release before any writer emits it, so the fleet can be upgraded
and rolled back freely inside that window. That is a property of the release order, not of the bytes.

## Consequences

- **Moving the emitted version is a staged deployment step**, not a code change that can ride any
  release: every Worker must decode the new version before any Worker writes it.
  `Continuation.schema_version` is what lets a writer be pinned behind its readers while that is
  arranged, and is why the version lives on the value rather than in the encoder.
- The binding is encoded **inline in each entry**, not appended after them. A trailing block is
  precisely what an unknowing reader steps over, so the layout that makes D possible is the layout to
  avoid; interleaved, the bytes are unreadable to anything that has not been taught the version.
- **Version 1 stays decodable**, and accepting it restores a cursor with the binding checks skipped.
  That is the same outcome an old Worker reaches and not the same fault: a check that was never
  recorded cannot be made, while a check that *was* recorded must never be discarded. The first is
  the residue of an upgrade and ends with the chain; the second is a Worker ignoring proof it was
  handed.
- **`provider_format_version` is read by map membership, not by truthiness.** The provider contract
  reserves no value, so zero is a version a provider may declare and the encoding represents it
  exactly. Read as a sentinel it silently skipped the comparison for that one value, which made
  Continue-As-New *less* safe than marker replay for the identical binding.
- The three binding fields are written for a wait together, so "recorded" means one thing rather than
  one thing per field.
- Restoration classifies a mismatch the way the annotation's binding table does: stream or backend
  name is nondeterminism, provider identity or format version is a storage failure
  (`spec/failure-taxonomy.md`). Both are raised before the cursor reaches the manager, so no backend
  reads at a boundary that was not produced against it.
- A compatibility test cannot be written against this Worker's own decoder — that is the one pair
  never mixed. It has to run an implementation frozen at the older behaviour over bytes this Worker
  produces, and **it must exercise that implementation's restoration, not only its decoder.** A
  decoder-level test of D's envelope passes: the old decoder returns the cursor, and that successful
  return *is* the unsafe behaviour rather than evidence against it.
