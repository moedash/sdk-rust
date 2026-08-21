# ADR-040 — A fence is ordered behind data writes, and behind nothing else

**Status:** Accepted · **Affects:** P6 · **Spec:** `spec/backend-contract.md`

## Context

`finish_writing()` claims that every write in this producer session preceding the fence has been
appended, and a consumer that drains through a fence may park on it. The claim is about *invocation*
order, because that is the order a caller wrote its calls in — and a `publish()` draws its sequence
number before awaiting the payload codec, deliberately, so that an idempotency key belongs to the
call rather than to whatever order an external payload store answers in (ADR-020, and "Producer
binding" in the spec).

So a publish invoked first can still be encoding when the fence is called, invisible to both the
backend (it has appended nothing) and the unresolved-append check (it has no unresolved append). A
fence appended there parks a consumer in front of data that was never written, and in a `wake=False`
batch it also spends the batch's only wake before the records exist.

Enforcing the claim needs a per-stream record of which calls are in flight, shared by every handle
`topic()` returned for the name, since the stream is one thing and the handle is not. The question
this record answers is *what belongs in that order*.

## Options

**A. Nothing.** Document that the fence is ordered by the sequence number it drew and leave the
append unordered.

**B. One order holding publishes and fences alike**, each call waiting on everything unsettled when
it began.

**C. An order of publishes only.** A fence reads it and waits behind it; a fence does not join it.

**D. One order, with a kind on each entry**, and a fence propagating failures only from the entries
marked as data.

## Decision

**C.**

A was the state a review found. The sequence number fixes the record's *place* in the stream and says
nothing about when it is sent, so the fence's own claim was the one thing the fence did not check.

B is the obvious enforcement and puts a second defect where the first was. A fence is not a data
write, and it has no record whose absence a later fence needs to know about — so an earlier fence
that never reached the backend, cancelled while it waited or refused, presents to a later fence
exactly as a publish whose record went missing. The later fence is then refused with
`PrecedingWriteFailedError`, an error documented as reporting a failed `publish()`, at a moment when
every write it claims is durable. Two fences also have no ordering to preserve between them: each
asserts something about the publishes it was invoked after, and neither assertion is inside the
other.

D is B with the symptom patched. It keeps fences in a structure whose only purpose is to be waited
on by fences, then adds a field so that they are skipped when they are. C deletes the entry instead
of learning to ignore it.

## Consequences

- Concurrent fences are unordered with respect to each other, and both claims are true whichever
  order the backend puts them in.
- A fence has no settled outcome for anything to read, so it neither registers nor settles. What
  remains of the fence path is: read the order, wait, append.
- **The fence reads every earlier outcome after its last wait, not as each one settles.** An
  append whose answer was lost is *unknown* rather than failed, and `resolve_append()` can turn that
  into a durable record or an `AppendConflictError` while the fence is still waiting on a later
  publish. An outcome read in the loop is a stale one, and the stale value it reads is the permissive
  one.
- For the same reason, **a resolution is reported to the operation a fence captured**, rather than
  left for the fence to infer from the record no longer being outstanding: resolving it durably and
  refusing it with a conflict both stop it being outstanding, so that inference reads a proven-absent
  write as a durable one and releases a fence over the hole.
- Where a fence has both an unresolved and a definitely-failed write ahead of it, the unresolved one
  is reported, because it blocks the recovery the other error prescribes — republishing is itself
  refused while an append on that stream is unsettled (ADR-038).
- A test for this needs a publish held inside its codec and *two* fences, with the first ending
  without appending: a single-fence test passes under B. A test for the resolution rule needs a
  backend that loses one answer and then refuses the recovery, which is contract behaviour on both
  halves rather than a broken store.
