package storage

import "testing"

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
