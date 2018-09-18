package hdfs

import (
	"flag"
	"os"

	"badoo/_packages/log"
	"github.com/badoo/lsd/util"
	"context"
	"path"
	"strings"
	"time"

	"fmt"

	"path/filepath"

	"os/exec"

	"github.com/cenkalti/backoff"
)

const FILES_BUFFER_SIZE = 100
const ERROR_SLEEP_INTERVAL = time.Second * 5

func showHelp() {
	fmt.Printf("usage: %s <flags> <absolute path to lsd category>\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(255)
}

func Transfer() {

	credentials := flag.String("namenode", "", "namenode credentials user@host:port")
	dstDir := flag.String("dst-dir", "", "destination dir in hdfs")
	tmpDir := flag.String("tmp-dir", "", "temporary buffer dir in hdfs")
	numWorkers := flag.Int("num-workers", 1, "how many parallel workers do upload")
	doDelete := flag.Bool("delete", true, "whether delete processed file")
	flag.Parse()

	if *credentials == "" || *dstDir == "" || *tmpDir == "" {
		showHelp()
	}
	credentialsList := strings.SplitN(*credentials, "@", 2)
	if len(credentialsList) < 2 || strings.Index(credentialsList[1], ":") == -1 {
		showHelp()
	}

	log.SetOutput(os.Stderr)
	log.SetLevel(log.InfoLevel)

	cmd := exec.Command("hostname", "-f")
	out, err := cmd.Output()
	if err != nil {
		log.Fatalf("failed to get hostname -f: %v", err)
	}
	hostname := strings.TrimSuffix(string(out), "\n")

	uploader, err := newUploader(hostname, credentialsList[0], credentialsList[1], *dstDir, *tmpDir)
	if err != nil {
		log.Fatalf("failed to init hdfs uploader: %v", err)
	}

	categoryPattern := flag.Arg(0)
	if categoryPattern == "" {
		log.Fatalf("glob pattern for absolute path to category is not specified")
	}
	categories, err := filepath.Glob(categoryPattern)
	if err != nil {
		log.Fatalf("failed to glob for pattern %s: %v", categoryPattern, err)
	}
	for _, categoryPath := range categories {
		st, err := os.Stat(categoryPath)
		if err != nil {
			log.Fatalf("failed to stat %s: %v", categoryPath, err)
		}
		if !st.IsDir() {
			log.Fatalf("%s is not a directory", categoryPath)
		}
		go transferCategory(categoryPath, *numWorkers, *doDelete, uploader)
	}
	select {}
}

func transferCategory(categoryPath string, numWorkers int, shouldDelete bool, u *uploader) {

	filesCh := make(chan string, FILES_BUFFER_SIZE)
	// sending to hdfs
	for i := 0; i < numWorkers; i++ {
		go func() {
			for {
				fileName := <-filesCh

				doUpload(u, fileName)

				fmt.Println(fileName)

				if shouldDelete {
					doDelete(fileName)
				}
			}
		}()
	}
	// reading from lsd
	baseDir, categoryName := path.Split(categoryPath)
	category := util.Category{BaseDir: baseDir, Name: categoryName}
	for {
		err := util.Watch(context.TODO(), category, filesCh)
		if err != nil {
			log.Errorf("lsd watch for %v failed: %v", category, err)
			time.Sleep(ERROR_SLEEP_INTERVAL)
		}
	}
}

func doUpload(u *uploader, name string) {
	// all files are the same for uploader,
	// so we should try as much as needed with constant backoff
	// to upload each one before getting next
	// if we get stuck for long, it means that something really went wrong
	backoff.RetryNotify(
		// upload to hdfs
		func() error {
			return u.upload(name)
		},
		backoff.NewConstantBackOff(ERROR_SLEEP_INTERVAL),
		func(err error, d time.Duration) {
			log.Errorf("upload failed: %v retry in %.2f seconds", err, d.Seconds())
		},
	)
}

func doDelete(name string) {
	backoff.RetryNotify(
		func() error {
			err := os.Remove(name)
			if os.IsNotExist(err) {
				return nil
			}
			return err
		},
		backoff.NewConstantBackOff(ERROR_SLEEP_INTERVAL),
		func(err error, d time.Duration) {
			log.Errorf("delete failed: %v retry in %.2f seconds", err, d.Seconds())
		},
	)
}
