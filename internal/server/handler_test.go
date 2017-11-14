package server

import (
	"badoo/_packages/gpbrpc"
	"badoo/_packages/log"
	"github.com/badoo/lsd/proto"
	"io/ioutil"
	"testing"

	"os"

	"github.com/badoo/lsd/internal/traffic"

	"time"

	"github.com/golang/protobuf/proto"
)

var tm *traffic.Manager

func TestSimple(t *testing.T) {
	dir, err := ioutil.TempDir("", "lsd_test")
	if err != nil {
		t.Fatalf("can't create temp dir: %s", err)
	}
	defer os.RemoveAll(dir)

	handler, err := NewHandler(&lsd.LsdConfigServerConfigT{
		TargetDir:          proto.String(dir),
		MaxFileSize:        proto.Uint64(10),
		FileRotateInterval: proto.Uint64(10),
	}, getTrafficManager())
	if err != nil {
		t.Fatalf("handler create failed: %s", err)
	}
	defer handler.Shutdown()

	testString := "test1\r\n"

	event := lsd.RequestNewEventsEventT{
		Category: proto.String("test"),
		Inode:    proto.Uint64(1),
		Offset:   proto.Uint64(2),
		Lines:    []string{testString},
	}
	resp := handler.RequestNewEvents(gpbrpc.RequestT{}, &lsd.RequestNewEvents{Events: []*lsd.RequestNewEventsEventT{&event}})
	offsets := getResponse(t, resp)
	if len(offsets.Offsets) != 1 {
		t.Fatalf("bad response %v", offsets)
	}

	if offsets.Offsets[0].GetInode() != 1 {
		t.Errorf("bad inode %d, want 1", offsets.Offsets[0].GetInode())
	}

	if offsets.Offsets[0].GetOffset() != 2 {
		t.Errorf("bad offset %d, want 2", offsets.Offsets[0].GetOffset())
	}

	checkFile(t, testString, dir+"/test/test_current")
}

func TestNoRotate(t *testing.T) {
	dir, err := ioutil.TempDir("", "lsd_test")
	if err != nil {
		t.Fatalf("can't create temp dir: %s", err)
	}
	defer os.RemoveAll(dir)

	handler, err := NewHandler(&lsd.LsdConfigServerConfigT{
		TargetDir:          proto.String(dir),
		MaxFileSize:        proto.Uint64(10000),
		FileRotateInterval: proto.Uint64(10000),
	}, getTrafficManager())
	if err != nil {
		t.Fatalf("handler create failed: %s", err)
	}
	defer handler.Shutdown()

	var curContent string

	for _, testContent := range []string{"test1\r\n", "test2\r\n", "test3\r\n"} {
		curContent += testContent
		event := lsd.RequestNewEventsEventT{
			Category: proto.String("test"),
			Inode:    proto.Uint64(1),
			Offset:   proto.Uint64(2),
			Lines:    []string{testContent},
		}
		handler.RequestNewEvents(gpbrpc.RequestT{}, &lsd.RequestNewEvents{Events: []*lsd.RequestNewEventsEventT{&event}})

		curName := dir + "/test/test_current"
		checkFile(t, curContent, curName)
	}
}

func TestRotateBySize(t *testing.T) {
	dir, err := ioutil.TempDir("", "lsd_test")
	if err != nil {
		t.Fatalf("can't create temp dir: %s", err)
	}
	defer os.RemoveAll(dir)

	handler, err := NewHandler(&lsd.LsdConfigServerConfigT{
		TargetDir:          proto.String(dir),
		MaxFileSize:        proto.Uint64(3),
		FileRotateInterval: proto.Uint64(100),
	}, getTrafficManager())
	if err != nil {
		t.Fatalf("handler create failed: %s", err)
	}
	defer handler.Shutdown()

	for _, testContent := range []string{"test1\n", "test2\n", "test3\n"} {
		event := lsd.RequestNewEventsEventT{
			Category: proto.String("test"),
			Inode:    proto.Uint64(1),
			Offset:   proto.Uint64(2),
			Lines:    []string{testContent},
		}
		handler.RequestNewEvents(gpbrpc.RequestT{}, &lsd.RequestNewEvents{Events: []*lsd.RequestNewEventsEventT{&event}})

		curName := dir + "/test/test_current"
		checkFile(t, testContent, curName)
	}
}

func TestRotateByTime(t *testing.T) {
	dir, err := ioutil.TempDir("", "lsd_test")
	if err != nil {
		t.Fatalf("can't create temp dir: %s", err)
	}
	defer os.RemoveAll(dir)

	handler, err := NewHandler(&lsd.LsdConfigServerConfigT{
		TargetDir:          proto.String(dir),
		MaxFileSize:        proto.Uint64(100),
		FileRotateInterval: proto.Uint64(1),
	}, getTrafficManager())
	if err != nil {
		t.Fatalf("handler create failed: %s", err)
	}
	defer handler.Shutdown()

	for _, testContent := range []string{"test1\n"} {
		event := lsd.RequestNewEventsEventT{
			Category: proto.String("test"),
			Inode:    proto.Uint64(1),
			Offset:   proto.Uint64(2),
			Lines:    []string{testContent},
		}
		handler.RequestNewEvents(gpbrpc.RequestT{}, &lsd.RequestNewEvents{Events: []*lsd.RequestNewEventsEventT{&event}})
		curName := dir + "/test/test_current"
		checkFile(t, testContent, curName)

		event = lsd.RequestNewEventsEventT{
			Category: proto.String("test"),
			Inode:    proto.Uint64(1),
			Offset:   proto.Uint64(2),
			Lines:    []string{testContent},
		}
		handler.RequestNewEvents(gpbrpc.RequestT{}, &lsd.RequestNewEvents{Events: []*lsd.RequestNewEventsEventT{&event}})
		checkFile(t, testContent+testContent, curName)
		time.Sleep(time.Second)
	}
}

func TestFileError(t *testing.T) {
	log.SetLevel(log.FatalLevel)
	dir, err := ioutil.TempDir("", "lsd_test")
	if err != nil {
		t.Fatalf("can't create temp dir: %s", err)
	}
	defer os.RemoveAll(dir)

	handler, err := NewHandler(&lsd.LsdConfigServerConfigT{
		TargetDir:          proto.String(dir),
		MaxFileSize:        proto.Uint64(100),
		FileRotateInterval: proto.Uint64(10),
		ErrorSleepInterval: proto.Uint64(2),
	}, getTrafficManager())
	if err != nil {
		t.Fatalf("handler create failed: %s", err)
	}
	defer handler.Shutdown()

	event := lsd.RequestNewEventsEventT{
		Category:     proto.String("test"),
		Inode:        proto.Uint64(1),
		Offset:       proto.Uint64(2),
		Lines:        []string{"trash\n"},
		IsCompressed: proto.Bool(true),
	}
	resp := handler.RequestNewEvents(gpbrpc.RequestT{}, &lsd.RequestNewEvents{Events: []*lsd.RequestNewEventsEventT{&event}})
	_, ok := resp.Message.(*lsd.ResponseGeneric)
	if !ok {
		t.Fatalf("return bad message %+v, want error generic", resp)
	}

	time.Sleep(time.Second * 3)

	event = lsd.RequestNewEventsEventT{
		Category: proto.String("test"),
		Inode:    proto.Uint64(1),
		Offset:   proto.Uint64(2),
		Lines:    []string{"trash2\n"},
	}
	resp = handler.RequestNewEvents(gpbrpc.RequestT{}, &lsd.RequestNewEvents{Events: []*lsd.RequestNewEventsEventT{&event}})
	offs := getResponse(t, resp)
	if len(offs.Offsets) != 1 {
		t.Fatalf("bad response")
	}

	if offs.Offsets[0].GetOffset() != 2 || offs.Offsets[0].GetInode() != 1 {
		t.Errorf("bad offset response: %+v", offs)
	}

	checkFile(t, "trash2\n", dir+"/test/test_current")
}

func getResponse(t *testing.T, resp gpbrpc.ResultT) *lsd.ResponseOffsets {
	offsets, ok := resp.Message.(*lsd.ResponseOffsets)
	if !ok {
		t.Fatalf("bad message %v", resp)
	}

	return offsets
}

func checkFile(t *testing.T, content string, fileName string) {
	msg, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("file read failed")
	}

	if string(msg) != content {
		t.Errorf("bad file content %s, want %s", string(msg), content)
	}
}

func getTrafficManager() *traffic.Manager {
	if tm != nil {
		return tm
	}
	tm = traffic.NewManager("")
	return tm
}
