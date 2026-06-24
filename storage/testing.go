package storage

import "testing"

func assert(t *testing.T, condition bool, message string) {
	t.Helper()
	if !condition {
		t.Error(message)
	}
}

func assertEqual[T comparable](t *testing.T, name string, actual, expected T) {
	t.Helper()
	if actual != expected {
		t.Errorf("%s: got %v, expected %v", name, actual, expected)
	}
}
