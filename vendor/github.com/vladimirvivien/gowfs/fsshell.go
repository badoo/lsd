package gowfs

import "os"
import "bytes"
import "fmt"
import "io"
import "path"
import "io/ioutil"

const MAX_UP_CHUNK int64 = 1 * (1024 * 1024) * 1024 // 1 GB.
const MAX_DOWN_CHUNK int64 = 500 * (1024 * 1024)    // 500 MB

type FsShell struct {
	FileSystem  *FileSystem
	WorkingPath string
}

// Appends the specified list of local files to the HDFS path.
func (shell FsShell) AppendToFile(filePaths []string, hdfsPath string) (bool, error) {

	for _, path := range filePaths {
		file, err := os.Open(path)

		if err != nil {
			return false, err
		}
		defer file.Close()

		data, _, err := slirpLocalFile(*file, 0)
		if err != nil {
			return false, err
		}

		_, err = shell.FileSystem.Append(bytes.NewBuffer(data), Path{Name: hdfsPath}, 0)
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

// Returns a writer with the content of the specified files.
func (shell FsShell) Cat(hdfsPaths []string, writr io.Writer) error {
	for _, path := range hdfsPaths {
		stat, err := shell.FileSystem.GetFileStatus(Path{Name: path})
		if err != nil {
			return err
		}
		//TODO add code to chunk super large files.
		if stat.Length < MAX_DOWN_CHUNK {
			readr, err := shell.FileSystem.Open(Path{Name: path}, 0, stat.Length, 4096)
			if err != nil {
				return err
			}
			io.Copy(writr, readr)
		}
	}
	return nil
}

// Changes the group association of the given hdfs paths.
func (shell FsShell) Chgrp(hdfsPaths []string, grpName string) (bool, error) {
	for _, path := range hdfsPaths {
		_, err := shell.FileSystem.SetOwner(Path{Name: path}, "", grpName)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

// Changes the owner of the specified hdfs paths.
func (shell FsShell) Chown(hdfsPaths []string, owner string) (bool, error) {
	for _, path := range hdfsPaths {
		_, err := shell.FileSystem.SetOwner(Path{Name: path}, owner, "")
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

// Changes the filemode of the provided hdfs paths.
func (shell FsShell) Chmod(hdfsPaths []string, perm os.FileMode) (bool, error) {
	for _, path := range hdfsPaths {
		_, err := shell.FileSystem.SetPermission(Path{Name: path}, perm)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

// Tests the existence of a remote HDFS file/directory.
func (shell FsShell) Exists(hdfsPath string) (bool, error) {
	_, err := shell.FileSystem.GetFileStatus(Path{Name: hdfsPath})
	if err != nil {
		if remoteErr, ok := err.(RemoteException); ok && remoteErr.JavaClassName == "java.io.FileNotFoundException" {
			return false, nil
		} else {
			return false, err /* a different err */
		}
	}
	return true, nil
}

// Copies one specified local file to the remote HDFS server.
// Uses default permission, blocksize, and replication.
func (shell FsShell) Put(localFile string, hdfsPath string, overwrite bool) (bool, error) {
	if _, err := os.Stat(localFile); os.IsNotExist(err) {
		return false, fmt.Errorf("File %v not found.", localFile)
	}

	file, err := os.Open(localFile)
	if err != nil {
		return false, err
	}
	defer file.Close()

	// put as a new remote file
	_, err = shell.FileSystem.Create(
		file,
		Path{Name: hdfsPath + "/" + µ(path.Split(localFile))[1].(string)},
		overwrite,
		134217728,
		3,
		0644,
		4096)

	if err != nil {
		return false, err
	}

	return true, nil
}

// Copies sepcified local files to remote HDFS server.
// The hdfsPath must be a directory (created if it does not exist).
// Uses default permission, blocksize, and replication.
func (shell FsShell) PutMany(files []string, hdfsPath string, overwrite bool) (bool, error) {
	// if multiple files, put in remote directory
	if len(files) > 1 {
		stat, err := shell.FileSystem.GetFileStatus(Path{Name: hdfsPath})

		// if remote dir missing, crete it.
		if remoteErr := err.(RemoteException); remoteErr.JavaClassName == "java.io.FileNotFoundException" {
			if _, err := shell.FileSystem.MkDirs(Path{Name: hdfsPath}, 0700); err != nil {
				return false, err
			}
		}
		if stat.Type == "FILE" {
			return false, fmt.Errorf("HDFS resource %s must be a directory in this context.", hdfsPath)
		}
	}
	for _, file := range files {
		shell.Put(file, hdfsPath+"/"+µ(path.Split(file))[1].(string), overwrite)
	}
	return true, nil
}

// Retrieves a remote HDFS file and saves as the specified local file.
func (shell FsShell) Get(hdfsPath, localFile string) (bool, error) {
	file, err := os.Create(localFile)
	if err != nil {
		return false, err
	}
	defer file.Close()

	reader, err := shell.FileSystem.Open(Path{Name: hdfsPath}, 0, 0, 0)
	if err != nil {
		return false, err
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return false, err
	}
	defer reader.Close()

	_, err = file.Write(data)
	if err != nil {
		return false, err
	}
	file.Sync()
	return true, nil
}

// Merges content of remote HDFS path into a single local file.
// func (shell FsShell) GetMerge(hdfsPath, localFile string)(bool, error){
// 	return false, fmt.Errorf("Function is unimplemented.")
// }

// Copies local file to remote destination, then local file is removed.
func (shell FsShell) MoveFromLocal(localFile, hdfsPath string, overwrite bool) (bool, error) {
	ok, err := shell.Put(localFile, hdfsPath, overwrite)
	// validate operation, then remove local
	if ok && err == nil {
		hdfStat, err := shell.FileSystem.GetFileStatus(Path{Name: path.Join(hdfsPath, path.Base(localFile))})
		if err != nil {
			return false, fmt.Errorf("Unable to verify remote file. err is %v", err.Error())
		}

		file, err := os.Open(localFile)
		if err != nil {
			return false, fmt.Errorf("Unable to validate operation. err is %v", err.Error())
		}
		if hdfStat.Length != µ(file.Stat())[0].(os.FileInfo).Size() {
			return false, fmt.Errorf("Remote and local file size mismatch.")
		}
		file.Close()               // close now.
		err = os.Remove(localFile) // remove it.
		if err != nil {
			return false, err
		}
	} else {
		return false, err
	}
	return true, nil
}

// Copies remote HDFS file locally.  The remote file is then removed.
func (shell FsShell) MoveToLocal(hdfsPath, localFile string) (bool, error) {
	hdfStat, err := shell.FileSystem.GetFileStatus(Path{Name: hdfsPath})
	_, err = shell.Get(hdfsPath, localFile)
	if err != nil {
		return false, err
	}

	file, err := os.Open(localFile)
	if err != nil {
		return false, fmt.Errorf("Unable to access local file %s: %s", localFile, err.Error())
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("Unable to access local file %s: %s", localFile, err.Error())
	}

	// ensure file was copied all the way
	if hdfStat.Length != fileStat.Size() {
		return false, fmt.Errorf("Local file size does not match remote file size. Aborting.")
	}

	// remove remote File
	ok, err := shell.FileSystem.Delete(Path{Name: hdfsPath}, false)
	if err != nil {
		return false, fmt.Errorf("Unable to remove remote %s file: %s", hdfsPath, err.Error())
	}

	return ok, nil
}

// Removes the specified HDFS source.
func (shell FsShell) Rm(hdfsPath string) (bool, error) {
	return false, fmt.Errorf("Function is unimplemented.")
}

// TODO: slirp file in x Gbyte chunks when file.Stat() >> X.
//       this is to avoid blow up memory on large files.
func slirpLocalFile(file os.File, offset int64) ([]byte, int64, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}

	if stat.Size() < MAX_UP_CHUNK {
		data, err := ioutil.ReadFile(file.Name())
		if err != nil {
			return nil, 0, err
		}
		return data, 0, nil
	} // else chunck it

	return nil, 0, nil
}

//TODO: slirp file in X GBytes chucks from server to avoid blowing up network.
// func slirpRemoteFile (hdfsPath string, offset int64, totalSize int64)([]byte, int64, error) {

// }
