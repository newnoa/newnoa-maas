# Codex native fallback model prompt

Unmodified `codex-rs/models-manager/prompt.md` from OpenAI Codex tag `rust-v0.169.0`.
Source: https://github.com/openai/codex/tree/rust-v0.169.0/codex-rs/models-manager

The tagged `model_info.rs` loads this file as `BASE_INSTRUCTIONS`. The model catalog schema requires either `base_instructions` or `model_messages.instructions_template` for every row. NewNoa embeds the upstream fallback to preserve the native instructions when building a custom authorized catalog. Existing native instruction overrides continue to be applied by Codex. This asset introduces no runtime dependency. Upstream LICENSE and NOTICE are retained alongside it.

Frozen source commit: `d32063f96c2dff1dc96af839b3952abb6acfeda4` (peeled `rust-v0.169.0`). Prompt SHA-256: `ac8ae107a0d72fe3476b430afb161ea4e67da2e446d778aefc44828160559807`.
