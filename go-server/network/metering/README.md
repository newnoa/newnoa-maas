# Shared transport content meter

Frozen API and caller ACK barrier: [DESIGN.md](DESIGN.md).
This package estimates observable text; it does not authorize calls, choose
billable evidence, persist checkpoints, cancel transports or settle points.

## Caller contract

Construct with API-authorized actual model ID and logical upstream request.
`Feed` returns only complete parsed SSE batches, preserving wire bytes. Hold all
returned bytes until the matching `Snapshot` is durably acknowledged by API.
Partial events stay inside the meter. Nonstream JSON releases only at `Finish`.
If Feed/Finish returns an error, release no bytes from that call; retain Snapshot
as evidence and close the transport. No inference retry. Each Meter is single-owner.
Returned Snapshot contains counters/provenance, never prompt/output text.

Supported observation protocols: Chat Completions, Responses, Messages and ASR
JSON/SSE. Unsupported protocol paths return ErrProtocol. Do not blanket-enable on
images, embeddings, rerank or systemone without a separate adapter/contract; their
media or custom output tokens cannot be invented from this package. Multipart ASR
input is not a JSON logical request; pass nil for unavailable input rather than
labeling file bytes as tokens. Text-only request estimates exclude chat envelope
and provider-specific overhead; multimodal/unknown content makes input unavailable.

Known explicit model registry uses cl100k_base/o200k_base; unknown aliases use
reference o200k counts with encoding=unknown and estimator=reference-o200k-v1.
Neither is authoritative provider billing. Hidden reasoning and cache quantities
are never inferred. Reported partial usage stays separate and is not replaced by
local counts; valid failed-terminal usage is retained without forwarding errors.
A Messages initial output zero does not by itself become terminal actual usage.

Bounds: 8MiB SSE event/nonstream JSON, 8MiB cumulative retained decoded output,
4096 logical lanes. Exact recounts retain full bounded lanes for terminal snapshot comparison; no approximate suffix,
per-chunk ceiling or long-run splitting. A codec-specific DEL omission still in v0.10.0
is corrected only at its proven single-byte merge barrier (rank 221, no multi-byte
vocabulary entry contains DEL). Parent independently verified that invariant and
supplied control-context oracle cases. Codec instances are exclusively pooled.

## Initial verification (v0.8.1, 2026-11-14)

- Go **1.26.4** with cached dependencies, `GOPROXY=off`:
  `go test -race -json -count=1 -timeout=120s ./metering`: **1,093 test/subtest passes,
  zero failures, zero skips**, 2.806 seconds, no race diagnostics.
- Official Python tiktoken **0.14.0 encode_ordinary** fixture is parent-owned:
  **506 inputs × 2 encodings**, full token-ID comparison, not length-only.
- Parent independent protocol fixture: seven fixtures × three model cases ×
  twelve transport-partition seeds, including bytewise; official expected counts.
- Additional regressions: Unicode/escaped surrogates, limits, invalid numeric usage,
  random partitions and CR/LF/BOM, multiple choices/tools, snapshot copies and
  decreasing BPE counts, partial cache retention, failed-terminal usage, summary
  indices/done/terminal snapshot deduplication, concurrent codec leases.
- Golden and protocol fixtures plus parent protocol_oracle_test.go were not edited.
  No Python oracle implementation or AGPL relay source was copied into this package.
- Final full network module `go test -race -json -count=1 -timeout=180s ./...`
  on Go1.26.4 passed all three packages: **1,142 test/subtest passes**, zero
  failures, four existing optional native-process skips (no exclusions). The meter
  package has no skips. `go vet ./...`, gofmt and diff checks pass. Raw race JSONL, selected red
  reproductions, frozen source/oracle hashes and benchmark results are in
  [Server verification evidence](../../docs/verification/transport-metering-network-20261114/).

## Performance boundary

### Long-piece CPU fix (v0.10.0)

Final pinned Go1.26.4 `GOPROXY=off go test -race -json -count=1 -timeout=180s ./...`:
all three network packages pass, **1,169 test/subtest passes, zero failures,
four existing optional native-process skips**, no exclusions. Meter alone has
**1,120 passes, zero skips**, 18.186s. `go vet ./...`, module verification,
gofmt/diff checks and unchanged parent-oracle hashes pass.

Pinned official MIT `github.com/tiktoken-go/tokenizer v0.10.0`, tag commit
`ff13c9398c8cb8606553f923b49e66fe9d51ca2e`. Go1.26 requirement and codec API
are compatible. The official heap merge path starts at 96 bytes; no new BPE
implementation, pretoken length cap or arbitrary text slicing was added here.
The DEL workaround remains necessary: this version's direct codec still drops
DEL, while the independent 506-vector control-character corpus verifies our wrapper.

An additional official Python tiktoken0.14.0 `encode_ordinary` oracle compares
full IDs for ten inputs in both encodings: 95/96/97-byte threshold boundaries,
16KiB/70K/250K/0.5MiB repeated `a`, alternating text, long CJK and whitespace.
Concurrent 70K counts and 70K/250K SSE/nonstream byte-preservation tests cover the
caller boundary. Parent-owned original tokenizer/protocol fixtures are unchanged.

One-iteration repeated-`a` Count benchmarks, Go1.26.4 on i5-11400:

| Piece bytes | cl100k normal / race | o200k normal / race |
|---|---:|---:|
| 70,000 | 25.7ms / 185ms | 21.5ms / 164ms |
| 250,000 | 77.5ms / 652ms | 83.4ms / 650ms |
| 524,288 | 167ms / 1.47s | 178ms / 1.47s |

The old v0.8.1 70K full-ID test timed out after five seconds under race with its
stack in the quadratic merge loop. The new dependency passes the same test and
the larger exact-ID cases. Raw before/after evidence is outside the module in
the linked verification directory. These measurements are not a universal SLA.

### Cumulative recount boundary

Pinned Go1.26.4, i5-11400, 128KiB mixed English/CJK/code corpus, one iteration:

| Delivery shape | Time | Total allocated bytes |
|---|---:|---:|
| 256-byte event per Feed + Snapshot | 3.667s | ~2.05GB |
| 8KiB event per Feed + Snapshot | 99.6ms | ~77.2MB |
| Same 256-byte events, coalesced into 8KiB reads | 131ms | ~123MB |
| Unchanged cached Snapshot only | 374ns | 16 bytes |

These are a measured limitation, not a universal SLA. Counts are recalculated
once per changed lane per Feed batch, Snapshot is cached, but tiny separate Feed
calls still have cumulative recount cost. Adapters should feed complete events
already coalesced in their transport read; they may not forward before checkpoint
ACK. Extremely long unbroken BPE pieces can also be expensive in the dependency.
No approximate token accounting was introduced to hide that cost. Further exact
incremental optimization needs proof and independent oracle/benchmark coverage.
The v0.10.0 rerun measured 3.762s for 256-byte Feed calls, 145ms for 8KiB
events, and 190ms for small events coalesced into 8KiB reads. Heap merging fixes
large individual pieces, not repeated recount of many short pieces.

No callers/vendor refresh, API schema, prices/settlement, commit, runtime binary,
restart/deploy or port18225 operations were performed by this package owner.

### Exact prefix reuse (2026-11-20)

With the two pinned codec regexes an ASCII letter, a single ASCII space and
an ASCII letter guarantee a complete piece boundary immediately before the
space. BPE merges are confined to each regex piece. Counts before this boundary
are immutable under append; recount only the open suffix. Never split on a
fixed byte window. A basic Han letter followed by one of `，。！？；：、`
also guarantees a new piece before the punctuation. For an open run of basic
Han or lowercase ASCII letters with an optional leading space or one of these
punctuation marks, both regexes produce one piece. Cache a whole UTF-8
token and its preceding count. On append, re-encode starting at that token
and reuse the earlier count only if the first token is reproduced exactly.
This preserves BPE merge order across the seam. If the token changes, the
whole open run is encoded again. All other open suffixes retain full recount.
This does not change the wire,
ACK barrier, estimator or billing rules. Recheck the proof on any codec upgrade.
