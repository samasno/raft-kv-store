package storage

import (
	"bytes"
	"testing"

	"github.com/samasno/raft-kv/raft"
)

func TestOpenLogfile(t *testing.T) {
	dir := t.TempDir()
	lf, err := OpenLogFile(dir)
	if err != nil {
		t.Fatal(err.Error())
	}

	ismagic, err := readMagicNumber(lf.entriesfilep)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !ismagic {
		t.Fatal("no magic in entries")
	}

	ismagic, err = readMagicNumber(lf.indexfilep)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !ismagic {
		t.Fatal("no magic in index")
	}

	lf.Close()

	// reopen log file
	lf, err = OpenLogFile(dir)

	ismagic, err = readMagicNumber(lf.entriesfilep)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !ismagic {
		t.Fatal("no magic in entries on reopen")
	}

	ismagic, err = readMagicNumber(lf.indexfilep)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !ismagic {
		t.Fatal("no magic in index")
	}

	lf.Close()
}

func TestAppendEntries(t *testing.T) {
	testEntries := raft.GenerateEntries(300, 0, 1)

	dir := t.TempDir()

	lf, err := OpenLogFile(dir)
	if err != nil {
		t.Fatal(err.Error())
	}

	err = lf.AppendEntries(testEntries)
	if err != nil {
		t.Fatalf("Failed to append: %s", err.Error())
	}

	assertEqual(t, "Updated tailIndex index", lf.tailIndex.Index, 300)
	assertEqual(t, "Updated tailindex term", lf.tailIndex.Term, 1)
	lf.Close()

	lf, err = OpenLogFile(dir)
	lastLogIndex, _ := lf.LastLogIndex()
	lastTerm, _ := lf.LastLogTerm()
	assertEqual(t, "Got last index", lastLogIndex, 300)
	assertEqual(t, "Got last term", lastTerm, 1)
}

func TestFetchIndex(t *testing.T) {
	testEntries := raft.GenerateEntries(300, 0, 1)

	dir := t.TempDir()

	lf, err := OpenLogFile(dir)
	if err != nil {
		t.Fatal(err.Error())
	}

	err = lf.AppendEntries(testEntries)
	if err != nil {
		t.Fatalf("Failed to append: %s", err.Error())
	}

	testIndex := uint64(33)
	index, err := lf.fetchIndex(testIndex)
	if err != nil {
		t.Fatal(err.Error())
	}

	assertEqual(t, "Fetched correct inded", index.Index, testIndex)
}

func TestGetEntry(t *testing.T) {
	testEntries := raft.GenerateEntries(300, 0, 1)

	dir := t.TempDir()

	lf, err := OpenLogFile(dir)
	if err != nil {
		t.Fatal(err.Error())
	}

	err = lf.AppendEntries(testEntries)
	if err != nil {
		t.Fatalf("Failed to append: %s", err.Error())
	}

	assertEqual(t, "Updated tailIndex index", lf.tailIndex.Index, 300)
	assertEqual(t, "Updated tailindex term", lf.tailIndex.Term, 1)

	testIndex := uint64(1)
	entry, _ := lf.GetEntry(testIndex)

	assertEqual(t, "Pulled correct index", entry.Index, testIndex)
}

func TestIndexSerializes(t *testing.T) {
	index := LogIndex{
		Index:         100,
		Term:          3,
		Offset:        3991292,
		PayloadLength: 100,
	}

	input := bytes.NewBuffer(index.Marshall())

	buf := LogIndex{}
	recoveredIndex, _ := buf.Unmarshall(input)
	assertEqual(t, "Recovered same Index", recoveredIndex.Index, index.Index)
	assertEqual(t, "Recovered same term", recoveredIndex.Term, index.Term)

	assertEqual(t, "Fixed length as expected", len(index.Marshall()), indexFixedSize)
}
