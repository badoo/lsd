package gowfs

import "fmt"
import "os"
import "net/http"
import "strconv"

// Renames the specified path resource to a new name.
// See HDFS FileSystem.rename()
func (fs *FileSystem) Rename(source Path, destination Path) (bool, error) {
	if source.Name == "" || destination.Name == "" {
		return false, fmt.Errorf("Rename() - params source and destination cannot be empty.")
	}

	params := map[string]string{"op": OP_RENAME, "destination": destination.Name}
	u, err := buildRequestUrl(fs.Config, &source, &params)
	if err != nil {
		return false, err
	}

	req, _ := http.NewRequest("PUT", u.String(), nil)
	hdfsData, err := requestHdfsData(fs.client, *req)
	if err != nil {
		return false, err
	}

	return hdfsData.Boolean, nil
}

//Deletes the specified path.
//See HDFS FileSystem.delete()
func (fs *FileSystem) Delete(path Path, recursive bool) (bool, error) {
	if path.Name == "" {
		return false, fmt.Errorf("Delete() - param path cannot be empty.")
	}
	params := map[string]string{
		"op":        OP_DELETE,
		"recursive": strconv.FormatBool(recursive)}

	u, err := buildRequestUrl(fs.Config, &path, &params)
	if err != nil {
		return false, err
	}

	req, _ := http.NewRequest("DELETE", u.String(), nil)
	hdfsData, err := requestHdfsData(fs.client, *req)
	if err != nil {
		return false, err
	}

	return hdfsData.Boolean, nil
}

// Sets the permission for the specified path.
// See FileSystem.setPermission()
func (fs *FileSystem) SetPermission(path Path, permission os.FileMode) (bool, error) {
	if path.Name == "" {
		return false, fmt.Errorf("SetPermission() - param path cannot be empty.")
	}
	if permission < 0 || permission > 1777 {
		return false, fmt.Errorf("SetPermission() - permission is invalid.")
	}
	params := map[string]string{
		"op":         OP_SETPERMISSION,
		"permission": strconv.FormatInt(int64(permission), 8)}

	u, err := buildRequestUrl(fs.Config, &path, &params)
	if err != nil {
		return false, err
	}

	req, _ := http.NewRequest("PUT", u.String(), nil)
	rsp, err := fs.client.Do(req)
	if err != nil {
		return false, err
	}
	if rsp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("SetPermission() - server returned unexpected value, permission not set.")
	}

	return true, nil
}

//Sets owner for the specified path.
//See HDFS FileSystem.setOwner()
func (fs *FileSystem) SetOwner(path Path, owner string, group string) (bool, error) {
	if path.Name == "" {
		return false, fmt.Errorf("SetOwner() - param path cannot be empty.")
	}
	params := map[string]string{
		"op":    OP_SETOWNER,
		"owner": owner,
		"group": group}

	u, err := buildRequestUrl(fs.Config, &path, &params)
	if err != nil {
		return false, err
	}

	req, _ := http.NewRequest("PUT", u.String(), nil)
	rsp, err := fs.client.Do(req)
	if err != nil {
		return false, err
	}
	if rsp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("SetOwner() - server returned unexpected value, owner not set.")
	}

	return true, nil
}

// Sets replication factor for given path.
// See HDFS FileSystem.setReplication()
func (fs *FileSystem) SetReplication(path Path, replication uint16) (bool, error) {
	if path.Name == "" {
		return false, fmt.Errorf("SetReplication() - param path cannot be empty.")
	}
	if replication <= 0 {
		return false, fmt.Errorf("SetReplication() - replication is invalid.")
	}
	params := map[string]string{
		"op":          OP_SETREPLICATION,
		"replication": strconv.FormatInt(int64(replication), 8)}

	u, err := buildRequestUrl(fs.Config, &path, &params)
	if err != nil {
		return false, err
	}
	req, _ := http.NewRequest("PUT", u.String(), nil)
	hdfsData, err := requestHdfsData(fs.client, *req)
	if err != nil {
		return false, err
	}

	return hdfsData.Boolean, nil
}

// Sets access or modification time for specified resource
// See HDFS FileSystem.setTimes
func (fs *FileSystem) SetTimes(path Path, accesstime int64, modificationtime int64) (bool, error) {
	if path.Name == "" {
		return false, fmt.Errorf("SetTimes() - Path cannot be empty.")
	}

	params := map[string]string{
		"op":               OP_SETTIMES,
		"accesstime":       strconv.FormatInt(int64(accesstime), 10),
		"modificationtime": strconv.FormatInt(int64(modificationtime), 10)}

	u, err := buildRequestUrl(fs.Config, &path, &params)
	if err != nil {
		return false, err
	}

	req, _ := http.NewRequest("PUT", u.String(), nil)
	rsp, err := fs.client.Do(req)
	if err != nil {
		return false, err
	}
	if rsp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("SetTimes() - server returned unexpected value, resource times not modified.")
	}

	return true, nil
}

// Creates the specified directory(ies).
// See HDFS FileSystem.mkdirs()
func (fs *FileSystem) MkDirs(p Path, fm os.FileMode) (bool, error) {
	params := map[string]string{"op": OP_MKDIRS}

	if fm < 0 || fm > 1777 {
		params["permission"] = "0700"
	} else {
		params["permission"] = strconv.FormatInt(int64(fm), 8)
	}
	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return false, err
	}

	req, _ := http.NewRequest("PUT", u.String(), nil)
	hdfsData, err := requestHdfsData(fs.client, *req)
	if err != nil {
		return false, err
	}

	return hdfsData.Boolean, nil
}

// Creates a symlink where link -> destination
// See HDFS FileSystem.createSymlink()
// dest - the full path of the original resource
// link - the symlink path to create
// createParent - when true, parent dirs are created if they don't exist
// See http://hadoop.apache.org/docs/r2.2.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#HTTP_Query_Parameter_Dictionary
func (fs *FileSystem) CreateSymlink(dest Path, link Path, createParent bool) (bool, error) {
	params := map[string]string{"op": OP_CREATESYMLINK}

	if dest.Name == "" || link.Name == "" {
		return false, fmt.Errorf("CreateSymlink - param dest and link cannot be empty.")
	}

	params["destination"] = dest.Name
	params["createParent"] = strconv.FormatBool(createParent)
	u, err := buildRequestUrl(fs.Config, &link, &params)
	if err != nil {
		return false, err
	}

	req, _ := http.NewRequest("PUT", u.String(), nil)
	rsp, err := fs.client.Do(req)

	defer rsp.Body.Close()

	if err != nil {
		return false, err
	}

	return true, nil
}

// Returns status for a given file.  The Path must represent a FILE
// on the remote system. (see HDFS FileSystem.getFileStatus())
func (fs *FileSystem) GetFileStatus(p Path) (FileStatus, error) {
	params := map[string]string{"op": OP_GETFILESTATUS}
	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return FileStatus{}, err
	}

	req, _ := http.NewRequest("GET", u.String(), nil)
	hdfsData, err := requestHdfsData(fs.client, *req)
	if err != nil {
		return FileStatus{}, err
	}

	return hdfsData.FileStatus, nil
}

// Returns an array of FileStatus for a given file directory.
// For details, see HDFS FileSystem.listStatus()
func (fs *FileSystem) ListStatus(p Path) ([]FileStatus, error) {

	params := map[string]string{"op": OP_LISTSTATUS}
	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return nil, err
	}

	req, _ := http.NewRequest("GET", u.String(), nil)
	hdfsData, err := requestHdfsData(fs.client, *req)
	if err != nil {
		return nil, err
	}

	return hdfsData.FileStatuses.FileStatus, nil
}

//Returns ContentSummary for the given path.
//For detail, see HDFS FileSystem.getContentSummary()
func (fs *FileSystem) GetContentSummary(p Path) (ContentSummary, error) {
	params := map[string]string{"op": OP_GETCONTENTSUMMARY}
	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return ContentSummary{}, err
	}

	req, _ := http.NewRequest("GET", u.String(), nil)
	hdfsData, err := requestHdfsData(fs.client, *req)
	if err != nil {
		return ContentSummary{}, err
	}

	return hdfsData.ContentSummary, nil
}

func (fs *FileSystem) GetHomeDirectory() (Path, error) {
	return Path{}, fmt.Errorf("Method GetHomeDirectory(), not implemented yet.")
}

// Returns HDFS file checksum.
// For detail, see HDFS FileSystem.getFileChecksum()
func (fs *FileSystem) GetFileChecksum(p Path) (FileChecksum, error) {
	params := map[string]string{"op": OP_GETFILECHECKSUM}
	u, err := buildRequestUrl(fs.Config, &p, &params)
	if err != nil {
		return FileChecksum{}, err
	}

	req, _ := http.NewRequest("GET", u.String(), nil)
	hdfsData, err := requestHdfsData(fs.client, *req)
	if err != nil {
		return FileChecksum{}, err
	}
	return hdfsData.FileChecksum, nil
}
