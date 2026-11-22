# Shared observed-content meter — provisional contract/test spec

2026-11-14. Owner scope: this package and network/go.mod/go.sum only; the approved
Python oracle generator lives in root python-tools. No Core/gateway caller edits.
Contract approved by parent `docs/superpowers/plans/2026-11-14-transport-metering.md`.

## Frozen caller API / wire contract

- `New(model, protocol string, requestBytes []byte) (*Meter, error)` uses actual
  API-authorized model ID, original logical request and upstream protocol path.
- `Feed(raw []byte) (ready []byte, err error)` returns complete parsed SSE batches
  byte-for-byte; partial suffix remains internal. On error ready is nil even if
  earlier complete events in that batch contributed durable evidence.
- `Snapshot() Snapshot` returns immutable plaintext-free cumulative observations.
- `Finish() (ready []byte, err error)` parses/releases a complete nonstream JSON
  document or finishes SSE; repeated Finish is idempotent. Feed after finish fails.
- Checkpoint Snapshot and validate API ACK **before** forwarding any ready bytes.
  On errors report retained observations separately and close upstream.
- Single-owner per Meter; codecs are leased exclusively from pools across meters.
- JSON tags exactly: estimator, encoding, estimatedInputTokens?,
  estimatedOutputTokens?, responseStarted, reported?{inputTokens?,outputTokens?,
  cacheWriteTokens?,cachedInputTokens?}, terminalUsage. Optional counters are
  `*int64`. Sequence belongs to reporters; BPE counts can decrease.
- `responseStarted` requires parsed text/tool content or valid reported counters,
  not headers, empty created frames or an error envelope without usage.
- Estimators: known registry `tiktoken-visible-v1`; unknown model
  `reference-o200k-v1` with encoding `unknown`. Both are local content estimates,
  not provider billing truth. No word heuristic, bytes ratio or alias guessing.

Bounds: MaxEventBytes=8MiB (SSE event or JSON body), MaxContentBytes=8MiB total
retained decoded output, MaxLanes=4096. Bounds latch ErrLimit with no ready bytes,
retaining previous parsed evidence; no silent reset/restart. ErrProtocol,
ErrTruncated and ErrFinished distinguish failure classes. Reported usage keeps
partial categories separate from local estimates, including failed-terminal usage.

## Counting semantics

- Independent decoded logical lanes per choice/content block/tool ID/index;
  concatenate fragments **within the lane** before encoding. Do not concatenate
  independent choices or tools, and do not sum independently encoded network chunks.
- Content, exposed reasoning, tool names and argument fragments are distinct lanes.
  Summaries are not hidden reasoning and must not be called full reasoning tokens.
- Responses delta/done/terminal snapshot hierarchy avoids double counting:
  terminal/full-text snapshot fills absent lanes or verifies seen lanes; it does
  not append the full result after deltas. Same for Messages blocks and tool JSON.
- Literal special-token-looking text is ordinary user/model text, not control
  tokens. v0.10.0 must match Python `encode_ordinary` on pinned golden cases.
- Unknown model/encoding uses o200k reference BPE with explicit estimated provenance;
  no arbitrary alias gains a provider encoding. Count once per changed lane per Feed
  batch; Snapshot is cached. No per-chunk ceiling or approximate run splitting.
- Retain bounded decoded lane text for exact recount; bounded pending event/JSON
  buffer and maximum lane count. On bound violation latch a typed error and mark
  observation incomplete/unavailable rather than silently dropping bytes or
  pretending a truncated prefix is the whole completion. Limits and partial-bill
  semantics await contract. No whole unbounded stream buffer.
- Request text parsing excludes base64 images/audio/blob content; supported
  message/input/system/instructions and tools have explicit lane coverage. Unknown
  multimodal/custom shapes are unknown coverage, not invented input charges.

## TDD and oracle gates

1. Pin independent MIT `github.com/tiktoken-go/tokenizer v0.10.0`, check license,
   dependency sums and generated cl100k/o200k assets. No new-api/tokenkit source.
2. Root Python generator pins official OpenAI tiktoken version + wheel/source
   provenance, prewarms explicitly verified assets and emits frozen token IDs/counts
   for encode_ordinary. Generator is test-only, never imported at runtime.
3. Small independent corpus: English, CJK, Arabic/Indic, combining marks/NFC/NFD,
   emoji/ZWJ, code, JSON/URLs, numbers, whitespace, long repeated runs, literal
   special token strings, empty text. Socrates owns independent corpus review later.
4. Red tests first for exact cl100k/o200k token counts/IDs against Python fixtures,
   explicit unknown aliases and unsupported encodings, ordinary special strings.
5. Every byte boundary + randomized partitions of UTF-8, SSE LF/CR/CRLF/BOM,
   multiline data/JSON escapes/comments, stream/nonstream JSON fallback.
6. Chat multiple choices/tool fragment IDs, Responses output/content indices,
   terminal snapshots with/without deltas, Messages text/thinking/tool JSON;
   observed snapshots are allowed to decrease after a merge; sequence isn't count.
7. Clean terminal, incomplete EOF, malformed JSON, duplicate terminal, error event,
   after-Finish feeds, bound violations, overflow, request multimodal exclusions,
   malformed UTF-8 and partial tool JSON. Preserve parsed prefix with error facts.
8. Full tests/race/vet for network module; offline network-disabled test after cache
   warm proves runtime does not download. Benchmarks bound memory and repeated
   Snapshot costs; optimize only with correctness proof, never truncate as exact.

## Remaining caller-owned decisions

API owns reporting cadence, durable sequence/hash/checkpoints, whether partial evidence
is sufficient to settle, cache rate selection and final reconciliation. Gateway/Core
adapters own cancellation, useful-output ACK barrier and finalization. No callers,
schema, vendor refresh or settlement policy implemented in this package.
