package metering

import (
	"errors"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/tiktoken-go/tokenizer/codec"
)

// Explicit trusted-model registry v1. An arbitrary alias never grants an exact codec.
func encodingForModel(model string) string {
	switch model {
	case "gpt-4", "gpt-4-0314", "gpt-4-0613", "gpt-4-32k", "gpt-3.5-turbo", "gpt-3.5-turbo-0125", "gpt-35-turbo", "text-embedding-ada-002", "text-embedding-3-small", "text-embedding-3-large":
		return "cl100k_base"
	case "gpt-4o", "gpt-4o-2024-05-13", "gpt-4o-2024-08-06", "gpt-4o-2024-11-20", "gpt-4o-mini", "gpt-4o-mini-2024-07-18", "gpt-4.1", "gpt-4.1-mini", "gpt-4.1-nano", "gpt-5", "gpt-5-mini", "gpt-5-nano", "o1", "o3", "o3-mini", "o4-mini":
		return "o200k_base"
	default:
		return "unknown"
	}
}

var cl100k = sync.Pool{New: func() any { return codec.NewCl100kBase() }}
var o200k = sync.Pool{New: func() any { return codec.NewO200kBase() }}

// Independent MIT codec v0.10.0 encodes literal special-token strings as ordinary text.
func encodeOrdinary(encoding, text string) ([]uint, error) {
	if !utf8.ValidString(text) {
		return nil, errors.New("meter: invalid UTF-8")
	}
	var pool *sync.Pool
	switch encoding {
	case "cl100k_base":
		pool = &cl100k
	case "o200k_base":
		pool = &o200k
	default:
		return nil, errors.New("meter: unsupported encoding")
	}
	c := pool.Get().(*codec.Codec)
	defer pool.Put(c)
	// v0.10.0 regex still drops DEL. Both pinned vocabularies contain it only as byte
	// token 221, so it is an exact merge barrier, not a rolling-suffix heuristic.
	var ids []uint
	for i, part := range strings.Split(text, "\x7f") {
		if i > 0 {
			ids = append(ids, 221)
		}
		next, _, err := c.Encode(part)
		if err != nil {
			return nil, err
		}
		ids = append(ids, next...)
	}
	return ids, nil
}

func countText(encoding, text string) (int64, error) {
	if encoding == "unknown" {
		encoding = "o200k_base" // reference text estimate, never an alias/provider encoding claim
	}
	if !utf8.ValidString(text) {
		return 0, ErrProtocol
	}
	pool := &o200k
	if encoding == "cl100k_base" {
		pool = &cl100k
	} else if encoding != "o200k_base" {
		return 0, ErrProtocol
	}
	c := pool.Get().(*codec.Codec)
	defer pool.Put(c)
	var total int64
	for i, part := range strings.Split(text, "\x7f") {
		if i > 0 {
			total++
		}
		n, err := c.Count(part)
		if err != nil {
			return 0, err
		}
		total += int64(n)
	}
	return total, nil
}

// A lowercase ASCII or basic Han letter (optionally preceded by punctuation or a space)
// stays in the same regex piece as later lowercase/Han letters in both codecs.
func simpleWord(text string, atStart bool) bool {
	if atStart {
		r, width := utf8.DecodeRuneInString(text)
		if strings.ContainsRune(" ，。！？；：、", r) {
			text = text[width:]
		}
	}
	for _, r := range text {
		if !(r >= 'a' && r <= 'z') && !(r >= '\u4e00' && r <= '\u9fff') {
			return false
		}
	}
	return true
}

// Find a previous token consisting of complete UTF-8 letters, so its first
// byte is also a safe place to restart the regex. Leave a short margin to
// avoid frequent misses on growing words. Correctness comes from re-encoding
// the anchor, not from the margin.
func (l *lane) rememberWord(text string, pieces []string, start int, preceding int64) {
	l.wordToken = ""
	end := start
	for i, token := range pieces {
		begin := end
		end += len(token)
		if len(text)-end >= 16 && token != " " && utf8.ValidString(token) {
			l.wordStart, l.wordCount, l.wordToken = begin, preceding+int64(i), text[begin:end]
		}
	}
}

func (l *lane) countOpen(encoding, text string) (int64, error) {
	start := l.stableBytes
	addedAt := max(start, l.scanned)
	if !simpleWord(text[addedAt:], addedAt == start) {
		l.wordBlocked = true
	}
	if l.wordBlocked || len(text)-start < 128 {
		return countText(encoding, text[start:])
	}

	pool := &o200k
	if encoding == "cl100k_base" {
		pool = &cl100k
	}
	c := pool.Get().(*codec.Codec)
	defer pool.Put(c)

	if l.wordToken != "" {
		// Within one BPE piece, re-encoding from a token boundary reproduces
		// the token's ranked merges. If it survives on the right, neither side
		// can introduce the first merge across the boundary: the old encoding
		// already rejected each such competing merge during its construction.
		_, pieces, err := c.Encode(text[l.wordStart:])
		if err != nil {
			return 0, err
		}
		if len(pieces) > 0 && pieces[0] == l.wordToken {
			total := l.wordCount + int64(len(pieces))
			l.rememberWord(text, pieces, l.wordStart, l.wordCount)
			return total, nil
		}
	}
	_, pieces, err := c.Encode(text[start:])
	if err != nil {
		return 0, err
	}
	l.rememberWord(text, pieces, start, 0)
	return int64(len(pieces)), nil
}
