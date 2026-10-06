# ADR-028 — Decoding a record is split across the Worker/Workflow boundary

**Status:** Accepted · **Affects:** P4, P19, P13, P18 · **Spec:** `spec/python-runtime.md`

## Context

Turning a record's payload bytes into a value means running the Workflow's own `DataConverter`,
codec included, because producer and consumer must agree on one (ADR-015). The value's type comes
from the topic, and a topic is Workflow code — so the obvious place to decode is where the record is
handed over: the subscription iterator, on the Workflow thread.

That place cannot run a `DataConverter`. `DataConverter.decode` is three steps — external-payload
retrieval, the user's `PayloadCodec`, then `from_payloads` — and the first two are arbitrary
asynchronous work. On the Workflow thread they run inside `activate()`, under the deterministic
event loop: an `await` inside a codec becomes a **real Temporal timer command in Workflow
History**, so History depends on the codec's internals and replays only while that codec behaves
identically; a codec that blocks rather than awaits fails against the Workflow sandbox's
restrictions instead; and either way a multi-second round trip sits under the 2-second deadlock
timeout. A codec doing real I/O hits one of the three.

The observation that resolves it: **only the third step needs the type, and that step is
synchronous.**

## Options

**A. Split the decode at that seam.** Retrieval and the codec run on the Worker's loop before the
record reaches the Workflow thread; `from_payloads` runs there, with the topic's type.

**B. Decode on the Workflow thread, and send the asynchronous half back.** A delivery that finds an
unprepared record ends the activation, the Worker's loop prepares it, and readiness is re-notified.

**C. Decode on the Workflow thread, but hop.** Submit the asynchronous half to the Worker's loop
with `run_coroutine_threadsafe` and block the Workflow thread on the result.

**D. Keep the whole decode in the Workflow-facing API**, reaching through the runtime handle into
the manager to borrow the Worker's converter.

## Decision

**A.**

B costs **an extra Workflow Task per record** — the one cost this design spends everything else to
avoid — and it cannot serve replay at all. Replay's records are delivered from a marker's recorded
segments inside a single activation, so there is no readiness round trip to re-run between them, and
they must all be prepared before the first one is handed over.

C converts an await into a **guaranteed** deadlock-timeout failure rather than an occasional one.
The Workflow thread is the thread `activate()` runs on and the thread the 2-second timeout watches;
blocking it for a codec's round trip is precisely what that timeout exists to catch. Hopping changes
which thread waits, not whether the Workflow Task waits.

D leaves the asynchronous work on the Workflow thread — it changes where the converter comes from,
not where it runs — and it makes the Workflow-facing module depend on the manager's internals. That
module knows its runtime only as an opaque handle exposing named methods, which is what keeps every
Worker-side object out of Workflow code's reach; reaching past it for a field is the first crack in
that. Under A the handle grows one method instead, and what it hands back is a codec already bound
to the topic's type and capable only of the synchronous half.

## Consequences

- **Preparation has exactly two sites**, because a record reaches Workflow code exactly two ways:
  the watcher, before a record enters a subscription's buffer, and the replay plan's segments, when
  the replay job is prepared. A third delivery path would need a third preparation site, and one
  that forgot would be caught rather than tolerated — see the refusal below.
- **A preparation failure is data, not an exception.** It is carried on the record and raised by the
  delivery that would have yielded its value. Raised where it happened it would end the watcher for
  the whole Run, taking every later record with it, and would fail a Workflow Task over a record
  Workflow code might never have asked for.
- **An unprepared record on the Workflow thread is refused, not converted**, whenever the
  converter has a payload codec or external storage. A codec's output is just another payload, so
  the difference is invisible in the bytes and converting anyway yields a *plausible wrong value* —
  worse than a loud failure, because nothing reports it. A converter with neither has an empty
  asynchronous half and still converts correctly, so the refusal is conditional on the converter
  rather than on the path.
- **Preparation reaches two private `DataConverter` helpers** —
  `_external_retrieve_payload_sequence` and `_decode_payload_sequence`. `decode()` is those two
  calls followed by `from_payloads`, so the split does byte-for-byte the same work in the same
  order; but it is coupled to internals. A public seam exposing everything except the payload
  converter is the cleaner long-term home, and would make this a supported split rather than a
  tolerated one. The condition deciding whether an unprepared record may be converted is asked of
  the converter's two public members instead, because the equivalent private predicate is marked in
  its own source as temporary: a safety property must not stop holding the day an unrelated cleanup
  lands upstream.
- **The type never leaves Workflow code and the converter never enters it.** The manager prepares
  with no type hint at all — sound precisely because the payload converter is the only step that
  takes one.
- Every record read is prepared, including ones Workflow code never takes. That is the same bargain
  `decode_activation` already makes for an activation's payloads, and it costs one codec call per
  record later discarded.
