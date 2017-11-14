package health

import (
	"badoo/_packages/log"
	"badoo/_packages/service"
	"badoo/_packages/service/config"
	"github.com/badoo/lsd/internal/client"
	"github.com/badoo/lsd/internal/client/offsets"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/badoo/lsd/internal/client/files"

	"encoding/json"

	"github.com/badoo/lsd/proto"

	"io"

	"github.com/fsnotify/fsnotify"
	"golang.org/x/sync/errgroup"
)

const HEALTHCHECK_BUFFER_SIZE = 1000

type Result struct {
	Client ClientResult `json:"client"`
	Server ServerResult `json:"server"`
}

type ClientResult struct {
	Undelivered map[string]uint64 `json:"undelivered"`
	Errors      []string          `json:"errors"`
}

type ServerResult struct {
	Categories map[string]CategoryInfo `json:"categories"`
	Errors     []string                `json:"errors"`
}

type CategoryInfo struct {
	Count    uint64 `json:"count"`
	OldestTs int64  `json:"oldest_ts"`
}

func Check() {

	var configPath string
	flag.StringVar(&configPath, "c", "", "path to config file")
	flag.Parse()

	if configPath == "" {
		log.Fatalf("no config file specified with -c")
	}

	config := &struct {
		badoo_config.ServiceConfig
		lsd.LsdConfig
	}{}

	err := service.ParseConfigFromFile(configPath, config)
	if err != nil {
		log.Fatalf("failed to parse config '%s': %v", configPath, err)
	}
	// forcing logs to come only to output
	log.SetOutput(os.Stderr)
	log.SetLevel(log.Level(config.DaemonConfig.GetLogLevel()))

	res := Result{
		Client: checkClient(config.GetClientConfig()),
		Server: checkServer(config.GetServerConfig()),
	}
	jsonBytes, err := json.Marshal(res)
	if err != nil {
		log.Fatalf("failed to encode %v to json: %v", res, err)
	}
	fmt.Println(string(jsonBytes))
}

func checkClient(config *lsd.LsdConfigClientConfigT) ClientResult {

	result := ClientResult{
		Undelivered: make(map[string]uint64),
		Errors:      make([]string, 0),
	}
	if config == nil {
		return result
	}
	fatalErr := func() error {
		odb, err := offsets.InitDb(config.GetOffsetsDb())
		if err != nil {
			return fmt.Errorf("failed to load offsets db: %v", err)
		}

		wg := errgroup.Group{}

		sourceDir := strings.TrimRight(config.GetSourceDir(), string(os.PathSeparator))
		filesCh := make(chan fsnotify.Event, HEALTHCHECK_BUFFER_SIZE)
		wg.Go(func() error {
			defer close(filesCh)
			err = client.ReadDirIntoChannel(sourceDir, filesCh, true)
			if err != nil {
				return fmt.Errorf("failed to read %s files: %v", sourceDir, err)
			}
			return nil
		})
		wg.Go(func() error {
			for {
				event, ok := <-filesCh
				if !ok {
					return nil
				}
				err := func() error {
					st, err := files.Lstat(event.Name)
					if err != nil {
						return fmt.Errorf("failed to stat %s: %v", event.Name, err)
					}
					// we can have symlinks in workdir running in router/relay mode
					// just ignore them
					if st.IsDir() || st.IsLink() {
						return nil
					}
					entry, _ := odb.Get(st.Inode())
					if !entry.IsValid(event.Name) {
						// inode collision, no need to do anything with it
						return nil
					}
					diff := uint64(st.Size()) - entry.Offset
					if diff < 0 {
						return fmt.Errorf("file %s is delivered beyond it's offset: %d/%d", event.Name, st.Size(), entry.Offset)
					}
					result.Undelivered[client.ExtractCategoryName(sourceDir, event.Name)] += diff
					return nil
				}()
				if err != nil {
					result.Errors = append(result.Errors, err.Error())
				}
			}
		})
		return wg.Wait()
	}()
	if fatalErr != nil {
		result.Errors = append(result.Errors, fatalErr.Error())
	}
	return result
}

func checkServer(config *lsd.LsdConfigServerConfigT) ServerResult {
	result := ServerResult{
		Categories: make(map[string]CategoryInfo),
		Errors:     make([]string, 0),
	}
	if config == nil {
		return result
	}
	categories, err := func() ([]string, error) {
		rootFp, err := os.Open(config.GetTargetDir())
		if err != nil {
			return nil, fmt.Errorf("failed to open %s: %v", config.GetTargetDir(), err)
		}
		categoryDirs := make([]string, 0)
		for {
			entries, err := rootFp.Readdir(HEALTHCHECK_BUFFER_SIZE)
			if err == io.EOF {
				break
			}
			if err != nil {
				return categoryDirs, fmt.Errorf("failed to read %s: %v", config.GetTargetDir(), err)
			}
			for _, entry := range entries {
				if !entry.IsDir() {
					continue
				}
				categoryDirs = append(categoryDirs, entry.Name())
			}
		}
		return categoryDirs, nil
	}()
	if err != nil {
		result.Errors = append(result.Errors, err.Error())
		return result
	}
	for _, category := range categories {
		err := func() error {
			categoryPath := strings.TrimSuffix(config.GetTargetDir(), string(os.PathSeparator)) + string(os.PathSeparator) + category
			fp, err := os.Open(categoryPath)
			if err != nil {
				return fmt.Errorf("failed to open %s: %v", categoryPath, err)
			}
			info := CategoryInfo{}
			for {
				entries, err := fp.Readdir(HEALTHCHECK_BUFFER_SIZE)
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("failed to read dir %s: %v", categoryPath, err)
				}
				for _, entry := range entries {
					if !entry.Mode().IsRegular() {
						continue
					}
					mtime := entry.ModTime().Unix()
					if info.OldestTs == 0 || mtime > info.OldestTs {
						info.OldestTs = mtime
					}
					info.Count++
				}
			}
			result.Categories[category] = info
			return nil
		}()
		if err != nil {
			result.Errors = append(result.Errors, err.Error())
		}
	}
	return result
}
