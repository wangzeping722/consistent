package consistent

import (
	"log"
	"sort"
	"testing"
	"testing/quick"
)

func checkNum(num, expected int, t *testing.T) {
	if num != expected {
		t.Errorf("got %d, expected %d", num, expected)
	}
}

func TestNew(t *testing.T) {
	x := New()
	if x == nil {
		t.Fatalf("expected obj")
		return
	}
	checkNum(x.NumberOfReplicas, 20, t)
}

func TestAdd(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	checkNum(len(x.circle), 20, t)
	checkNum(len(x.sortedHashes), 20, t)
	if sort.IsSorted(x.sortedHashes) == false {
		t.Errorf("expected sorted hashed to be sorted")
		return
	}
	x.Add("qwer")
	checkNum(len(x.circle), 40, t)
	checkNum(len(x.sortedHashes), 40, t)
	if sort.IsSorted(x.sortedHashes) == false {
		t.Errorf("expected sorted hashed to be sorted")
		return
	}
}

func TestRemove(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	checkNum(len(x.circle), 20, t)
	checkNum(len(x.sortedHashes), 20, t)
	if sort.IsSorted(x.sortedHashes) == false {
		t.Errorf("expected sorted hashed to be sorted")
		return
	}
	x.Remove("abcdefg")
	checkNum(len(x.circle), 0, t)
	checkNum(len(x.sortedHashes), 0, t)
	if sort.IsSorted(x.sortedHashes) == false {
		t.Errorf("expected sorted hashed to be sorted")
		return
	}
}

func TestRemoveNonExisting(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Remove("aaa")
	checkNum(len(x.circle), 20, t)
}

func TestGetEmpty(t *testing.T) {
	x := New()
	_, err := x.Get("abcdefg")
	if err == nil {
		t.Errorf("expected error")
		return
	}
	if err != ErrEmptyCircle {
		t.Errorf("expected empty circle error")
		return
	}
}

func TestGetSingle(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	f := func(s string) bool {
		y, err := x.Get(s)
		if err != nil {
			t.Logf("err: %q", err)
			return false
		}
		t.Logf("s = %q, y = %q", s, y)
		return y == "abcdefg"
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

type gtest struct {
	in  string
	out string
}

var gmtests = []gtest {
	{"ggg", "abcdefg"},
	{"hhh", "opqrstu"},
	{"iiiii", "hijklmn"},
}

func TestGetMultiple(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Add("hijklmn")
	x.Add("opqrstu")
	for i, v := range gmtests {
		result, err := x.Get(v.in)
		if err != nil {
			t.Fatal(err)
		}
		if result != v.out {
			t.Errorf("%d. got %q, expected %q", i, result, v.out)
		}
	}
}

func TestGetMultipleQuick(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Add("hijklmn")
	x.Add("opqrstu")
	f := func(s string) bool {
		y, err := x.Get(s)
		if err != nil {
			t.Logf("err: %q", err)
			return false
		}
		t.Logf("s = %q, y = %q", s, y)
		return y == "abcdefg" || y == "hijklmn" || y == "opqrstu"
	}

	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}

var rtestBefore = []gtest {
	{"ggg", "abcdefg"},
	{"hhh", "opqrstu"},
	{"iiiii", "hijklmn"},
}

var rtestAfter = []gtest {
	{"ggg", "abcdefg"},
	{"hhh", "opqrstu"},
	{"iiiii", "opqrstu"},
}

func TestGetMultipleRemove(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Add("opqrstu")
	x.Add("hijklmn")

	for i, v := range rtestBefore {
		result, err := x.Get(v.in)
		if err != nil {
			log.Fatal(err)
		}
		if result != v.out {
			t.Errorf("%d. got %q, expected %q before rm", i, result, v.out)
		}
	}

	x.Remove("hijklmn")
	for i, v := range rtestAfter {
		result, err := x.Get(v.in)
		if err != nil {
			log.Fatal(err)
		}
		if result != v.out {
			t.Errorf("%d. got %q, expected %q after rm", i, result, v.out)
		}
	}
}

func TestGetTwoQuick(t *testing.T) {
	x := New()
	x.Add("abcdefg")
	x.Add("opqrstu")
	x.Add("hijklmn")

	f := func(s string) bool {
		a, b, err := x.GetTwo(s)
		if err != nil {
			t.Logf("error: %q", err)
			return false
		}
		if a == b {
			t.Logf("a == b")
			return false
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Fatal(err)
	}
}


func TestSet(t *testing.T) {
	x := New()
	x.Add("abc")
	x.Add("def")
	x.Add("ghi")
	x.Set([]string{"jkl", "mno"})
	if x.count != 2 {
		t.Errorf("expected 2 elts, got %d", x.count)
	}
	a, b, err := x.GetTwo("qwerqwerwqer")
	if err != nil {
		t.Fatal(err)
	}
	if a != "jkl" && a != "mno" {
		t.Errorf("expected jkl or mno, got %s", a)
	}
	if b != "jkl" && b != "mno" {
		t.Errorf("expected jkl or mno, got %s", b)
	}
	if a == b {
		t.Errorf("expected a != b, they were both %s", a)
	}
	x.Set([]string{"pqr", "mno"})
	if x.count != 2 {
		t.Errorf("expected 2 elts, got %d", x.count)
	}
	a, b, err = x.GetTwo("qwerqwerwqer")
	if err != nil {
		t.Fatal(err)
	}
	if a != "pqr" && a != "mno" {
		t.Errorf("expected pqr or mno, got %s", a)
	}
	if b != "pqr" && b != "mno" {
		t.Errorf("expected pqr or mno, got %s", b)
	}
	if a == b {
		t.Errorf("expected a != b, they were both %s", a)
	}
	x.Set([]string{"pqr", "mno"})
	if x.count != 2 {
		t.Errorf("expected 2 elts, got %d", x.count)
	}
	a, b, err = x.GetTwo("qwerqwerwqer")
	if err != nil {
		t.Fatal(err)
	}
	if a != "pqr" && a != "mno" {
		t.Errorf("expected pqr or mno, got %s", a)
	}
	if b != "pqr" && b != "mno" {
		t.Errorf("expected pqr or mno, got %s", b)
	}
	if a == b {
		t.Errorf("expected a != b, they were both %s", a)
	}
}
