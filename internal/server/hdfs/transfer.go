package hdfs

import (
	"flag"
	"os"

	"badoo/_packages/log"
	"badoo/_packages/lsd"
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
const ERROR_SLEEP_INTERVAL = time.Second * 10

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

func transferCategory(categoryPath string, numWorkers int, doDelete bool, uploader *uploader) {

	filesCh := make(chan string, FILES_BUFFER_SIZE)
	// sending to hdfs
	for i := 0; i < numWorkers; i++ {
		go func() {
			for {
				fileName := <-filesCh
				err := backoff.Retry(
					func() error {
						return uploader.upload(fileName)
					},
					backoff.WithMaxTries(backoff.NewConstantBackOff(ERROR_SLEEP_INTERVAL), 3),
				)
				if err != nil {
					log.Errorf("failed to upload %s: %v", fileName, err)
					continue
				}
				_, err = fmt.Fprintln(os.Stdout, fileName)
				if err != nil {
					log.Errorf("failed to write to STDOUT: %v", err)
					continue
				}
				if !doDelete {
					continue
				}
				err = os.Remove(fileName)
				if err != nil && !os.IsNotExist(err) {
					log.Errorf("failed to delete %s: %v", fileName, err)
				}
			}
		}()
	}
	// reading from lsd
	baseDir, categoryName := path.Split(categoryPath)
	category := lsd.Category{BaseDir: baseDir, Name: categoryName}
	for {
		err := lsd.Watch(context.TODO(), category, filesCh)
		if err != nil {
			log.Errorf("lsd watch for %v failed: %v", category, err)
			time.Sleep(ERROR_SLEEP_INTERVAL)
		}
	}
}
