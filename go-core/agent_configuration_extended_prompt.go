package main

import _ "embed"

// The native catalog requires each row to retain Codex's baseline instructions.
//
//go:embed third_party/codex-model-prompt-0.169.0/prompt.md
var codexExtendedBaseInstructions string
