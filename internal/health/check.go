package health

import (
	"badoo/_packages/log"
	"badoo/_packages/service"
	"badoo/_packages/service/config"
	"github.com/badoo/lsd/internal/client"
	"github.com/badoo/lsd/internal/client/offsets"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/badoo/lsd/internal/client/files"

	"encoding/json"

	"github.com/badoo/lsd/proto"

	"io"

	"badoo/_packages/gpbrpc"
	"badoo/_packages/service/stats"

	"io/ioutil"
	"net/http"
	"time"

	"github.com/fsnotify/fsnotify"
	"golang.org/x/sync/errgroup"
)

const HEALTHCHECK_BUFFER_SIZE = 1000
const HEALTHCHECK_STATS_CONNECT_TIMEOUT = time.Second * 3
const HEALTHCHECK_STATS_REQUEST_TIMEOUT = time.Second * 3

type Result struct {
	IsRunning bool         `json:"is_running"`
	Client    ClientResult `json:"client"`
	Server    ServerResult `json:"server"`
}

type ClientResult struct {
	Undelivered map[string]uint64 `json:"undelivered"`
	BacklogSize int               `json:"backlog_size"`
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

	isUp, err := isDaemonUp(config.GetDaemonConfig().GetListen())
	if err != nil {
		log.Fatalf("failed to check that daemon is up: %v", err)
	}
	backlogSize, err := getBacklogSize(isUp, config.GetDaemonConfig())
	if err != nil {
		log.Fatalf("failed to get backlog size: %v", err)
	}

	res := Result{
		IsRunning: isUp,
		Client:    checkClient(config.GetClientConfig(), backlogSize),
		Server:    checkServer(config.GetServerConfig()),
	}
	jsonBytes, err := json.Marshal(res)
	if err != nil {
		log.Fatalf("failed to encode %v to json: %v", res, err)
	}
	fmt.Println(string(jsonBytes))
}

func isDaemonUp(listenRows []*badoo_config.ServiceConfigDaemonConfigTListenT) (bool, error) {

	for _, listen := range listenRows {
		if listen.GetProto() != "service-stats-gpb" {
			continue
		}
		port, err := extractPort(listen.GetAddress())
		if err != nil {
			return false, fmt.Errorf("invalid service-stats-gpb listen format: %v", err)
		}
		cli := gpbrpc.NewClient(
			"localhost:"+port,
			&badoo_service.Gpbrpc,
			&gpbrpc.GpbsCodec,
			HEALTHCHECK_STATS_CONNECT_TIMEOUT,
			HEALTHCHECK_STATS_REQUEST_TIMEOUT,
		)
		_, _, err = cli.Call(&badoo_service.RequestStats{})
		cli.Close()
		return err == nil, nil
	}
	return false, errors.New("no service-stats-gpb section in config")
}

func getBacklogSize(isUp bool, daemonConfig *badoo_config.ServiceConfigDaemonConfigT) (int, error) {
	if !isUp {
		return 0, nil
	}
	port, err := extractPort(daemonConfig.GetHttpPprofAddr())
	if err != nil {
		return 0, fmt.Errorf("invalid http pprof address: %v", err)
	}
	resp, err := http.Get("http://localhost:" + port + "/debug/vars")
	if err != nil {
		return 0, fmt.Errorf("http request failed: %v", err)
	}
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("http request failed with non-200 status code: %d", resp.StatusCode)
	}
	jsonResp, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read http response body: %v", err)
	}
	debugVars := &struct {
		BacklogSize int `json:"out_backlog_size"`
	}{}
	err = json.Unmarshal(jsonResp, debugVars)
	if err != nil {
		return 0, fmt.Errorf("failed to decode json respose: %v", err)
	}
	return debugVars.BacklogSize, nil
}

func checkClient(config *lsd.LsdConfigClientConfigT, backlogSize int) ClientResult {

	result := ClientResult{
		Undelivered: make(map[string]uint64),
		BacklogSize: backlogSize,
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
					if os.IsNotExist(err) {
						return nil
					}
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

func extractPort(s string) (string, error) {
	chunks := strings.SplitN(s, ":", 2)
	if len(chunks) < 2 {
		return s, fmt.Errorf("invalid address format address: %s", s)
	}
	return chunks[1], nil
}
