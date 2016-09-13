package client

import (
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestInode(t *testing.T) {
	exec.Command("sh", "-c", "sleep 10 >>test.tmp &").Run()
	defer os.Remove("test.tmp")
	defer os.Remove("test2.tmp")
	defer os.Remove("test3.tmp")

	time.Sleep(time.Second)
	st, _ := os.Stat("test.tmp")
	usedIno := getIno(st)

	checkMap := make(map[uint64]bool)
	checkMap[usedIno] = true
	t.Logf("test.tmp inode: %d, it must be used by sleep 10", usedIno)

	// Check that files opened by ourselves are ok
	fp, _ := os.OpenFile("test2.tmp", os.O_CREATE|os.O_RDWR, 0666)
	defer fp.Close()

	fi, err := fp.Stat()
	ourIno := getIno(fi)
	checkMap[ourIno] = true
	t.Logf("test2.tmp inode: %d, it is held open by us and thus must not be considered as used", ourIno)

	os.Create("test3.tmp")
	st, _ = os.Stat("test3.tmp")
	unusedIno := getIno(st)
	checkMap[unusedIno] = true
	t.Logf("test3.tmp inode: %d, it is not used anywhere so it must be present in output map", unusedIno)

	start := time.Now().UnixNano()
	res, err := getFreeFiles(checkMap)
	t.Logf("getFreeFiles took %.3f seconds", float64(time.Now().UnixNano()-start)/1.0e9)
	t.Logf("Result: %+v, error: %+v", res, err)

	if !res[ourIno] {
		t.Fatalf("Inode %d for test2.tmp is considered as used although it is our handle", ourIno)
	}

	if !res[unusedIno] {
		t.Fatalf("Inode %d for test3.tmp is considered as used although it was just created", unusedIno)
	}

	if len(res) != 2 {
		t.Fatalf("Unexpected result length: %d", len(res))
	}
}
