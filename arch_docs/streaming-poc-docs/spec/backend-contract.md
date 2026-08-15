# Backend contract

What a stream provider must implement to be registrable, and what the producer side must supply.

Owned by P2 (core interface + conformance suite), P2b (parking extension), P3/P3b (Redis),
P6/P6a/P6b (producer), P17 (registry).

## Required operations

A backend implementation must provide:

- Immutable, append-only records.
- Stable, totally ordered offsets within a stream.
- An **inclusive range read** over an explicit `[first_offset, last_offset]` pair, and a separate
  **exclusive** watch for records strictly after a boundary. These are two distinct operations —
  see "Cursor semantics".
- **A guarantee that a record's bytes cannot change once written.** Mandatory, checked at
  registration (ADR-003).
- Atomic or otherwise race-free coordination for parking and wakeup.
- Retention sufficient to re-read every record referenced by retained Workflow History.
- Idempotent coordination operations and detection of missing records.
- **Idempotent append that is idempotent on identity, not on key alone** (ADR-020). An append
  reusing an existing `(session_id, sequence)` with byte-identical content is a no-op returning
  the original offset; the same pair with *different* bytes must be rejected as an error.

Control records and data records share the same offset sequence. Control records are consumed by
the runtime and are not yielded to Workflow code.

## Cursor semantics

A cursor is a **position boundary**, not the identity of a record (ADR-002):

```text
cursor := BEGINNING
        | AFTER(last_consumed_offset)
```

`BEGINNING` is the provider's beginning-of-stream boundary and is not required to be the offset of
any real record. `AFTER(x)` names the boundary immediately following the record at offset `x`,
whether or not a record after `x` exists yet.

The boundary form maps directly onto the two primitives every provider must expose:

- **Live resume and watch read strictly after the boundary.** In Redis this is `XREAD BLOCK` from
  `last_consumed_offset`, or from the beginning sentinel `0-0` for `BEGINNING`. `XREAD` returns
  entries *strictly after* the supplied ID, which is exactly exclusive-after semantics.
- **Replay reads an explicit recorded range, never "from the cursor".** The annotation records
  each run's `first_offset` and `last_offset`, and replay issues an inclusive range read for
  precisely that range — in Redis, `XRANGE <first> <last>`. Replay never asks the backend what
  comes next; the answer is already in the marker.

A provider may represent the boundary as `(offset, inclusive | exclusive)` instead. What it may
not do is require the cursor token to be the offset of a record that does not exist yet.

**Offsets are compared by their provider's ordering rule, not lexically.** Redis IDs compare as
numeric `(milliseconds, sequence)` tuples; string comparison is wrong as soon as the millisecond
component changes width.

## Structural immutability is a registration precondition

**Every provider must guarantee that a record's bytes cannot change once written.** A backend that
cannot make the guarantee does not satisfy this contract and is rejected when it is registered on
the Worker (P17), rather than compensated for at runtime. Redis Streams qualifies: an entry can be
deleted by `XDEL` or removed by trimming, but its fields cannot be rewritten in place.

Given that guarantee, replay needs to detect exactly one class of damage — a record that is **no
longer there** — and offsets are sufficient for it. Deletion, trimming, and retention expiry are
the realistic failure modes in operation, and all of them are caught by the four range checks in
`annotation-format.md`.

What the assumption costs, stated plainly: if a provider silently violates immutability — a buggy
custom backend, or out-of-band surgery on the stream — replay will deliver the altered bytes as
though they were original, and no error is raised. That risk is accepted deliberately and is
bounded by the registration requirement and the conformance suite. `schema_version` leads the
annotation encoding, so a per-record content-hash mode can be introduced later without a format
break if a provider ever needs one. See ADR-003.

**A decode failure is not an integrity failure.** With immutability guaranteed, bytes present
within a validated range are exactly the bytes that were written, so a DataConverter or codec that
cannot decode them indicates a configuration mismatch between producer and consumer — not a
damaged stream (ADR-015). See `failure-taxonomy.md`.

## Conformance suite requirements

The suite is the deliverable, not the interface. It must contain, at minimum:

- a case that parks a consumer at the current tail, appends a record whose ID could not have been
  predicted, and resumes — a backend that requires a nameable next ID fails it;
- a case that fails a backend implementing the inclusive range read with **exclusive** semantics,
  since that error is invisible until the first replay;
- a case that fails a backend comparing offsets lexically, crossing a millisecond-width boundary;
- a case that fails a backend accepting an idempotency-key reuse with different bytes;
- park intents keyed by stream alone failing the two-subscription case (ADR-012); and
- a claim that never expires failing the leased-claim case.

## Parking operations

`install_park_intent`, `remove_park_intent`, `recheck`, `claim_park_generation`,
`current_park_generation`.

Intents are keyed **`(stream key, wait_id)`**, never by stream key alone (ADR-012), carrying the
cursor boundary, the `park_generation`, and the current Run ID as the intent's *value*.

The key does not include the current Run ID: `wait_id` is stable across a Continue-As-New chain
and the stream key already contains the first execution Run ID, so the key is unique within a
chain, and only one Run of a chain is live at a time. Carrying the Run ID as the value means a new
Run's intent deterministically replaces its predecessor's for the same key rather than
accumulating alongside it.

**Claims must be leased and renewable.** An unleased claim introduces a failure mode: a producer
that crashes between claiming and signaling strands the generation, and other producers conclude
the wake is already handled. A provider that implements `claim_park_generation` must therefore
expire claims and permit takeover; a provider that cannot must expose observe-only semantics and
let every producer signal idempotently.

## Producer binding

A producer needs five things, none of which it can infer (ADR-019):

- **The Workflow chain key**, including the first execution Run ID. `temporalio.activity.Info`
  exposes `workflow_run_id` but *not* the first execution Run ID, so an Activity cannot derive the
  key. The Workflow passes it to the producer explicitly — as an Activity argument, or through
  whatever channel a non-Temporal producer already uses — and the producer verifies it by
  describing the Workflow before its first append. Publishing under an unverified key is a
  configuration error, not a silent no-op.
- **A backend connection.** Workers register named backends; a plain process constructs a provider
  directly.
- **A Temporal client**, for the wake Signal.
- **The same `DataConverter`** the consuming Workflow uses, including any codec. A mismatch is
  detected at decode time on the consumer and surfaces as a distinct decode failure.
- **A stable producer session ID and sequence**, which is what makes append idempotent under
  Activity retry. Activities default it to a value derived from the Activity's identity so a
  retried attempt reuses it; plain processes must supply one, and the API requires it rather than
  defaulting to a fresh random value.

The stream name appears exactly once on the producer side, in `topic()`. `connect()` takes the
Workflow *chain* key — namespace, Workflow ID, first execution Run ID — and `topic(name)`
completes it into the full stream identity, so one connection serves several topics and no two
arguments can disagree about the name.

## Write-fence semantics

`finish_writing()` means:

> All writes in this producer session preceding the fence have been appended. If a consumer drains
> through the fence and no later record is immediately available, it may park now.

The fence is an ordered stream record, so its relationship to concurrent data is unambiguous:

```text
offset 100  data
offset 101  data
offset 102  WRITE_FENCE(producer-session-id)
offset 103  data from another producer
```

At offset 102, the runtime continues if offset 103 is already available; otherwise that
subscription becomes immediately parkable. The fence neither closes the stream nor asserts that
all producers are finished. A fence on one stream only marks that stream parkable — the Workflow
Task parks early only when every active subscription is immediately parkable; otherwise the idle
timeout remains authoritative.

## Retention

Records and control metadata must remain available and immutable for as long as any retained Run
in the Workflow-ID chain may replay them. Backend retention must therefore cover at least the
namespace's Temporal retention for that chain. Garbage collection is allowed only after the chain
is terminal and its applicable Temporal retention/replay window has elapsed, or after an explicit
stronger archival policy guarantees replayability.

This is an operational prerequisite, not a code deliverable. Violations surface as stream-integrity
failures.
