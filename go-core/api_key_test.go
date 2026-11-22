package main

import "testing"

func TestCustomAPIKeyFormat(t *testing.T) {
	for _, value := range []string{"my_custom-key_1234", "nn_abcdefghijklmnopqrstuvwxyz0123456789"} {
		if !validAPIKey(value) {
			t.Fatal("valid key rejected")
		}
	}
	for _, value := range []string{"short", "abcdefghijklmnop!", " abcdefghijklmnop"} {
		if validAPIKey(value) {
			t.Fatal("invalid key accepted")
		}
	}
}
