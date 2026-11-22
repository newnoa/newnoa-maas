package rpc

import (
	"net/http"
	"testing"
)

func TestImagesAreAStandardPostInferenceRoute(t *testing.T) {
	if !permitted(Head{Method: http.MethodPost, Path: "/v1/images/generations", ContentLength: 0}) {
		t.Fatal("images route must use the standard authenticated inference transport")
	}
	if permitted(Head{Method: http.MethodGet, Path: "/v1/images/generations", ContentLength: 0}) || permitted(Head{Method: http.MethodPost, Path: "/publication-probe/v1/images/generations", ContentLength: 0}) {
		t.Fatal("images method or publication permit boundary widened")
	}
}
