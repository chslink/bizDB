package gen

import (
	"testing"
)

func TestMatch(t *testing.T) {

	tables := []string{
		"test", "test1", "test2",
		"atest",
	}

	dst := Match(tables, "*")
	if len(dst) != 4 {
		t.Error("match error")
	}

	dst = Match(tables, "test*")
	if len(dst) != 3 {
		t.Error("match error")
	}
	dst = Match(tables, "test1")
	if len(dst) != 1 {
		t.Error("match error")
	}

	dst = Match(tables, "*test*")
	if len(dst) != 4 {
		t.Error("match error")
	}

	dst = Match(tables, "*test")
	if len(dst) != 2 {
		t.Error("match error")
	}
	dst = Match(tables, "a*")
	if len(dst) != 1 {
		t.Error("match error")
	}
	dst = Match(tables, "test?")
	if len(dst) != 2 {
		t.Error("match error")
	}

	dst = Match(tables, "test[1-2]")
	if len(dst) != 2 {
		t.Error("match error")
	}
	dst = Match(tables, "test")
	if len(dst) != 1 {
		t.Error("match error")
	}
}
