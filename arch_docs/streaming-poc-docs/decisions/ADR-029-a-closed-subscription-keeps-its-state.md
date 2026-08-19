# ADR-029 — A closed subscription keeps its recorded state

**Status:** Accepted · **Affects:** P9, P8, P10b, P15 · **Spec:** `spec/python-runtime.md`

## Context

`close()` ends a wait: iteration stops, the wait leaves the quiescent snapshot, and the Worker stops
serving it. The symmetric teardown is to undo what `subscribe()` did — drop the runtime's record of
the subscription along with the manager's — and that is what "the subscription is over" appears to
mean.

The runtime's record is not only a live wait, though. Two things are built from the set of registered
subscriptions at moments that have nothing to do with any individual wait still being open: the
annotation binding the runtime emits for each wait, which replay reads, and the per-wait cursor in
the Continue-As-New continuation, which a successor Run resumes from. Both are consulted at the
*Run's* boundaries. A Workflow that reads a stream to its fence and closes it reaches both of them
afterwards.

## Options

**A. Delete the state**, in the runtime and in the manager alike.

**B. Keep the runtime's state and mark it closed**; drop only the manager's.

**C. Delete the state and keep a separate record** of the binding and cursor a closed wait still
owes.

## Decision

**B.**

A is durably wrong in two directions at once, and in the ordinary case rather than an exotic one. A
wait with no binding appears in the annotation as a `wait_id` with no stream key, no backend, and no
start cursor, which replay reports as a wait the Workflow did not create (ADR-027) — against code
that never changed, for a Workflow whose only unusual act was to stop reading a stream it had
finished with. And a successor Run reconstructs that same wait from the same deterministic
`subscribe()` call, so a continuation missing its cursor restarts the stream at `BEGINNING` and
re-delivers every record the closed subscription consumed. Closing is the point at which a cursor is
most worth keeping, not least: nothing later will move it, so it is final rather than stale.

C keeps the same two fields under a second name. Every reader of the registered set becomes a reader
of two sets that must be merged, and a reader that consults only the live one is wrong only after a
Workflow has closed a subscription — rare, silent, and durable in a marker or in a successor's
starting position. One set with one flag makes the correct reading the default: a reader that never
looks at `closed` still reads the right binding and the right cursor.

## Consequences

- **The two halves of teardown are deliberately asymmetric.** The manager's entry holds resources — a
  watcher, a buffer, a backend connection, and possibly a park intent (ADR-030) — and is dropped in
  full. The runtime's entry holds the record of what the wait *was*, and is kept. They are not two
  copies of one thing, and only one of them costs anything to hold.
- **Closing changes exactly one property of the state**: the wait can never be blocked again. That is
  what keeps a closed wait out of Core's hands. A wait that re-entered the blocked set would be
  registered, retained for, and eventually parked on behalf of a coroutine that no longer exists, and
  only a wake would end the Workflow Task it held open.
- **The teardown is requested through the runtime handle, not started from the subscription.**
  `close()` runs on the Workflow thread, and a task created there is not scheduled at all, so the
  request would be lost without a trace and the watcher would keep running. This is ADR-011's rule
  reached from the other side: Worker-side work is not merely misplaced on the Workflow thread, it
  does not happen.
- `wait_id` is allocated from a per-Run counter and never reused, so a closed entry can never be
  confused with a live one and no reader has to tell them apart by identity.
- A Run that subscribes and closes repeatedly accumulates one kept entry per iteration. That is the
  growth its annotation already has — each new wait must be bound in it regardless — so retention
  introduces no new bound, and a Workflow whose subscription count grows without limit is already an
  annotation-size problem (ADR-007).
- The continuation and multi-stream tests must close a subscription and still find its binding in the
  annotation and its cursor in the continuation, because the failure of A is invisible until a
  Continue-As-New or a replay happens.
