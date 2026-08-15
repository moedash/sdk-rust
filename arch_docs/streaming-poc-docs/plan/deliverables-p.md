# Track P — Python, no Core dependency

Fully parallel with Track C. These are shippable and testable before any Core work
exists.

**P1 — Record model, offsets, control records**
`StreamRecord` (offset, kind ∈ {DATA, WRITE_FENCE}, payload bytes, producer session id,
sequence). Offset as an opaque, totally-ordered, serializable token. Control and data
records share one offset sequence; control records are runtime-consumed and never
yielded to Workflow code.
*Done when:* ordering and round-trip unit tests pass. No Temporal, no Redis.

**P2 — `StreamBackend` core interface + conformance suite** ⇢ P1
The core ABC: `append`, `read_range(first, last)` (**inclusive of both endpoints**),
`watch(after=boundary)` (**exclusive**), `compare_offsets`, and the provider's
`guarantees_immutability` declaration. Cursors are boundaries — `BEGINNING | AFTER(offset)`
— never the identity of a future record; no provider operation may require naming a
record that does not exist yet. See `spec/backend-contract.md`, ADR-002.
`append` is idempotent on `(session_id, sequence)` **and identity**: identical bytes are
a no-op returning the original offset, different bytes are an error (ADR-020).
The **conformance suite is the real deliverable** — a parametrized pytest suite encoding
the backend contract. It must include cases that fail a backend which implements the
inclusive range read with exclusive semantics, which compares offsets lexically, which
cannot resume a consumer parked at the tail, or which accepts an idempotency-key reuse
with different bytes.
A backend that does not declare `guarantees_immutability` is rejected by the P17
registry rather than tested here — the guarantee is a precondition for registration, not
a runtime mode (ADR-003).
*Done when:* the suite exists and a deliberately-broken stub backend fails it for the
right reasons.

**P2b — Parking extension to the provider interface + conformance suite** ⇢ P2
`install_park_intent`, `remove_park_intent`, `recheck`, `claim_park_generation`,
`current_park_generation`. Intents are keyed **`(stream key, wait_id)`**, carrying the
cursor boundary, `park_generation`, and current Run ID as the value; a stream-keyed
intent is a conformance failure, since two same-stream subscriptions would overwrite each
other (ADR-012). Claims are **leased and renewable**: a producer that crashes between
claiming and signaling must not strand the generation, so the suite includes a
claim-expiry and takeover case. A provider that cannot lease must expose observe-only
semantics instead.
*Done when:* the parking conformance suite exists, a stub with a non-expiring claim fails
it, and a stub keying intents by stream alone fails the two-subscription case.

**P3 — Redis Streams backend, core operations** ⇢ P2, X2
Offsets are Redis stream IDs, compared as numeric `(ms, seq)` tuples. `XADD` to append,
**`XRANGE <first> <last>` for the inclusive recorded-range read**, `XREAD BLOCK` for
exclusive watching only. `XREAD` cannot serve a replay read — it returns entries strictly
after the supplied ID. A cursor is stored as `BEGINNING` (watch from `0-0`) or
`AFTER(id)` (watch from `id`), so a consumer parked at the tail never has to name the ID
of the next `XADD`. Control records as reserved fields.
*Done when:* passes the P2 conformance suite unmodified against a real Redis.

**P3b — Redis parking operations** ⇢ P2b, P3
Park intent install/recheck/remove and leased generation claim, via Lua for atomicity.
*Done when:* passes the P2b conformance suite against a real Redis.

**P4 — Payload codec integration** ⇢ P1
Encode/decode through the Workflow's `DataConverter`; type carried by
`topic("tokens", type=str)`.
*Done when:* round-trip tests through the default converter including a custom type.

**P5 — Replay annotation codec** ⇢ P1
Versioned encoding of the `header / segment* / terminal` schema in
`spec/annotation-format.md`: provider identity and format version, per-`wait_id` stream
key, and explicit `start_cursor`; one segment per original activation; the observed global
cross-stream schedule encoded as **runs** — maximal consecutive deliveries from one stream
as `(wait_id, first_offset, last_offset, count, control_positions)` — plus
`segment_end_reason` and the terminal blocked snapshot. `ParkReason` is **not** encoded
here; it lives in the Core-readable marker envelope (ADR-008).
Both run endpoints are recorded because start-plus-count cannot detect a deletion inside a
range (ADR-006). `control_positions` is sparse. Positions are cursor boundaries
(`BEGINNING | AFTER(offset)`); run endpoints are record offsets; the two are distinct types
in the codec.
`segment*` and `run*`, not `+`: an activation that drained and found nothing is an empty
segment and must round-trip, and an annotation with no segments at all — a subscription to
an empty stream — must round-trip too (ADR-005).
Run encoding is what makes marker size scale with cross-stream schedule transitions instead
of with record count; a single-stream batch of 100,000 records must encode as one run. No
field in the encoding is per-record.
Enforces `MAX_ANNOTATION_BYTES` at encode time and raises `request_rollover` at the
high-water mark rather than growing past it (ADR-007).
Records the *observed* global schedule rather than relying on a fixed ordering rule, so
future scheduling policies stay open. `schema_version` leads the encoding so older markers
stay readable.
*Done when:* round-trip, golden-file (catching silent format drift), and budget tests —
including empty-segment and zero-segment round-trips, one asserting a large single-stream
batch encodes as one run, one asserting **encoded byte size** stays flat as a single-stream
batch grows by three orders of magnitude, and one asserting an alternating two-stream batch
triggers rollover rather than exceeding the budget — plus a test that a delta sequence
concatenates into the same annotation Core would have accumulated.

**P6 — Producer API: append and `finish_writing()`** ⇢ P3, P4, P6a
Idempotent append keyed by producer session + sequence, safe under Activity retry —
identical bytes are a no-op, different bytes under the same key are an error. Ordered write
fence. Callable from Activities and from plain non-Temporal processes. This deliverable
stops at the append: it does **not** provide the acknowledged-wake contract.
*Done when:* an Activity publishes N records plus a fence, and a standalone script reads
them back in order through the backend. **Independently useful and shippable before any
Core work.**

**P6a — Producer binding: `ExternalStreamProducer.connect()`** ⇢ P4
A producer-side handle distinct from the Workflow-side one, with every binding input
explicit: the Workflow **chain** key (namespace, Workflow ID, `first_execution_run_id`),
backend, `DataConverter`, and producer session ID. The stream name belongs to `topic(name)`
and appears nowhere else, so no two arguments can disagree about it (ADR-019).
`temporalio.activity.Info` exposes `workflow_run_id` but not the first execution Run ID, so
the key is passed in by the Workflow and verified by the producer with a describe call
before its first append. Activities derive a default session ID from their own identity so
retries reuse it; plain processes must supply one — no random default.
*Done when:* an Activity and a standalone script both publish under the same verified key, a
retried Activity attempt appends no duplicate, and a wrong first-execution Run ID fails
loudly.

**P6b — `publish()` acknowledged-wake semantics** ⇢ P6, P2b, P3b, C11, P14
The documented contract that `publish()` completes only once its wake step is acknowledged,
with the un-acknowledged state surfaced rather than hidden. This is what makes the "durable
producer" row of the wakeup-durability boundary true, and it cannot exist before the park
generation (P2b/P3b), the `WakeSignal` envelope (C11), and the signaling path (P14) do.
*Done when:* a producer whose wake step fails reports un-acknowledged rather than returning
success, and retrying completes it.

**P17 — Worker backend registry** ⇢ P2
`Worker(external_stream_backends={...})`: named provider instances constructed outside the
sandbox, with credentials, referenced from Workflow code by name only.
Registration **rejects any provider that does not declare `guarantees_immutability`**. This
is the single enforcement point for the design's central precondition, and it belongs here —
at Worker construction, loudly, before any Workflow can name the backend — rather than at
replay, quietly, after data has already been consumed (ADR-003).
*Done when:* a provider without the immutability declaration fails Worker construction with a
message naming the guarantee, a conforming one registers, and the sandbox rejects a direct
provider import from Workflow code. Naming a registered backend *from* Workflow code is P9's
criterion — the Workflow-facing API is not in this closure.

**P18 — `StreamIntegrityError` and `StreamDecodeError` with distinct metrics** ⇢ P1
The two error types and their metrics, plus the mechanical classification rule: a missing
offset or a range that fails validation is integrity loss; anything that fails *after* a
range validates is a decode failure (ADR-015). **No `workflow_failure_exception_types`
registration** — there is no terminal-failure opt-in (ADR-014).
Declared before the replay read path that raises them so the taxonomy is not invented
incidentally by the first caller.
*Done when:* both types exist with separate metrics, and a unit test asserts the
classification rule in both directions.
