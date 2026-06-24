package storage

import (
	"encoding/binary"
	"io"
	"os"
	"testing"
)

func TestNewMetadatafileUpdates(t *testing.T) {
	dir := t.TempDir()
	mf, err := OpenMetadataFile(dir)
	if err != nil {
		t.Fatal(err.Error())
	}

	votedFor, _ := mf.VotedFor()
	currentTerm, _ := mf.CurrentTerm()

	assertEqual(t, "currentTerm returns 0", currentTerm, 0)
	assertEqual(t, "votedFor returns 0", votedFor, 0)

	termUpdate := uint64(475)
	votedForUpdate := uint64(111)
	if err = mf.UpdateCurrentTerm(termUpdate); err != nil {
		t.Fatal(err.Error())
	}

	if err = mf.UpdateVotedFor(votedForUpdate); err != nil {
		t.Fatal(err.Error())
	}

	mfilename := mf.Filename()
	if err = mf.Close(); err != nil {
		t.Fatal(err.Error())
	}

	mfile, err := os.Open(mfilename)
	if err != nil {
		t.Fatal(err.Error())
	}

	magic, err := readMagicNumber(mfile)
	if err != nil {
		t.Fatal(err.Error())
	}

	if !magic {
		t.Fatal("Didn't recover magic number")
	}

	var recoveredTerm uint64
	var recoveredVote uint64

	mfile.Seek(int64(currentTermOffset), io.SeekStart)
	if err = binary.Read(mfile, binary.LittleEndian, &recoveredTerm); err != nil {
		t.Fatal(err.Error())
	}

	mfile.Seek(int64(votedForOffset), io.SeekStart)
	if err = binary.Read(mfile, binary.LittleEndian, &recoveredVote); err != nil {
		t.Fatal(err.Error())
	}

	assertEqual(t, "Recovered vote from metadatafile", recoveredVote, votedForUpdate)
	assertEqual(t, "Recovered term", recoveredTerm, termUpdate)
}

func TestUseExistingMetadataFile(t *testing.T) {
	dir := t.TempDir()
	mf, err := OpenMetadataFile(dir)
	if err != nil {
		t.Fatal(err.Error())
	}

	votedFor, _ := mf.VotedFor()
	currentTerm, _ := mf.CurrentTerm()

	assertEqual(t, "currentTerm returns 0", currentTerm, 0)
	assertEqual(t, "votedFor returns 0", votedFor, 0)

	termUpdate := uint64(1000)
	votedForUpdate := uint64(999)
	if err = mf.UpdateCurrentTerm(termUpdate); err != nil {
		t.Fatal(err.Error())
	}

	if err = mf.UpdateVotedFor(votedForUpdate); err != nil {
		t.Fatal(err.Error())
	}

	if err = mf.Close(); err != nil {
		t.Fatal(err.Error())
	}

	reusedMf, err := OpenMetadataFile(dir)
	if err != nil {
		t.Fatal(err.Error())
	}

	recoveredTerm, _ := reusedMf.CurrentTerm()
	recoveredVote, _ := reusedMf.VotedFor()

	assertEqual(t, "Reusing existing term", recoveredTerm, termUpdate)
	assertEqual(t, "Reusing existing vote", recoveredVote, votedForUpdate)
}
