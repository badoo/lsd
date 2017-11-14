package offsets

import (
	"badoo/_packages/log"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

// used only to store in json and
// backwards compatibility with previous version
type DbEntry struct {
	Ino          uint64
	Offset       uint64 `json:"Off"`
	LastFileName string
}

func (e DbEntry) IsValid(name string) bool {
	if e.LastFileName == "" {
		// backwards compatibility
		return true
	}
	return strings.HasPrefix(name, e.LastFileName)
}

type Db struct {
	saveLock sync.Mutex
	lock     sync.RWMutex
	data     map[uint64]DbEntry
	path     string
}

func InitDb(path string) (*Db, error) {

	odb := &Db{
		path: path,
		data: make(map[uint64]DbEntry),
	}

	odb.lock.Lock()
	defer odb.lock.Unlock()

	jsonBytes, err := ioutil.ReadFile(path)
	if os.IsNotExist(err) {
		return odb, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %v", path, err)
	}

	var jsonData []DbEntry
	err = json.Unmarshal(jsonBytes, &jsonData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %v", err)
	}

	for _, entry := range jsonData {
		odb.data[entry.Ino] = entry
	}

	return odb, nil
}

func (odb *Db) IsEmpty() bool {
	odb.lock.RLock()
	res := len(odb.data) == 0
	odb.lock.RUnlock()
	return res
}

func (odb *Db) Get(ino uint64) (DbEntry, bool) {
	odb.lock.RLock()
	res, ok := odb.data[ino]
	odb.lock.RUnlock()
	return res, ok
}

func (odb *Db) SetOffset(ino uint64, offset uint64) error {
	odb.lock.Lock()
	entry, ok := odb.data[ino]
	if !ok {
		odb.lock.Unlock()
		return fmt.Errorf("no offsets db entry for inode %d", ino)
	}
	entry.Offset = offset
	odb.data[ino] = entry
	odb.lock.Unlock()
	return nil
}

func (odb *Db) Set(ino uint64, entry DbEntry) {
	odb.lock.Lock()
	odb.data[ino] = entry
	odb.lock.Unlock()
}

func (odb *Db) Delete(ino uint64) {
	odb.lock.Lock()
	delete(odb.data, ino)
	odb.lock.Unlock()
}

func (odb *Db) GetAllInodes() []uint64 {
	odb.lock.Lock()
	res := make([]uint64, 0, len(odb.data))
	for inode := range odb.data {
		res = append(res, inode)
	}
	odb.lock.Unlock()
	return res
}

func (odb *Db) Save() error {

	odb.saveLock.Lock()
	defer odb.saveLock.Unlock()

	tmpPath := odb.path + ".tmp"
	fp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("could not create tmp file for offsets db: %v", err)
	}
	defer func() {
		err := fp.Close()
		if err != nil {
			log.Errorf("failed to close %s: %v", fp.Name(), err)
		}
	}()

	odb.lock.RLock()
	jsonData := make([]DbEntry, 0, len(odb.data))
	for _, entry := range odb.data {
		jsonData = append(jsonData, entry)
	}
	odb.lock.RUnlock()

	jsonBytes, err := json.Marshal(jsonData)
	if err != nil {
		return fmt.Errorf("failed to marshal offsets db data to json: %v", err)
	}
	_, err = fp.Write(jsonBytes)
	if err != nil {
		return fmt.Errorf("failed to write json into tmp file %s: %v", tmpPath, err)
	}

	err = os.Rename(tmpPath, odb.path)
	if err != nil {
		return fmt.Errorf("failed to rename %s to %s: %v", tmpPath, odb.path, err)
	}
	return nil
}
