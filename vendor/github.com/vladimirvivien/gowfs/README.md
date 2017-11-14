[![Build Status](https://drone.io/github.com/vladimirvivien/gowfs/status.png)](https://drone.io/github.com/vladimirvivien/gowfs/latest)

## gowfs 
gowfs is a Go bindings for Hadoop HDFS via its WebHDFS interface.  It provides typed access to remote HDFS resources via Go's JSON marshaling system.  gowfs follows the WebHDFS JSON protocol outline in  http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html.  It has been tested with Apache Hadoop 2.x.x - series.

#### GoDoc Package Documentation
GoDoc documentation - https://godoc.org/github.com/vladimirvivien/gowfs

### Usage
```
go get github.com/vladimirvivien/gowfs
```
```go
import github.com/vladimirvivien/gowfs
...
fs, err := gowfs.NewFileSystem(gowfs.Configuration{Addr: "localhost:50070", User: "hdfs"})
if err != nil{
	log.Fatal(err)
}
checksum, err := fs.GetFileChecksum(gowfs.Path{Name: "location/to/file"})
if err != nil {
	log.Fatal(err)
}
fmt.Println (checksum)
```

### Run HDFS Test
To see the API used, see directory `test-hdfs`. Compile and use that code to test against a running  HDFS deployment.  See https://github.com/vladimirvivien/gowfs/tree/master/test-hdfs.

#### HDFS Setup
* Enable `dfs.webhdfs.enabled` property in your hsdfs-site.xml 
* Ensure `hadoop.http.staticuser.user` property is set in your core-site.xml.


## API Overview
gowfs lets you access HDFS resources via two structs `FileSystem` and `FsShell`.  Use FileSystem to get access to low level callse.  FsShell is designed to provide a higer level of abstraction and integration with the local file system.

### FileSystem API 
#### Configuration{} Struct
Use the `Configuration{}` struct to specify paramters for the file system.  You can create configuration either using a `Configuration{}` literal or using `NewConfiguration()` for defaults. 

```
conf := *gowfs.NewConfiguration()
conf.Addr = "localhost:50070"
conf.User = "hdfs"
conf.ConnectionTime = time.Second * 15
conf.DisableKeepAlives = false 
```

#### FileSystem{} Struct
Create a new `FileSystem{}` struct before you can make call to any functions.  You create the FileSystem by passing in a `Configuration` pointer as shown below. 
```
fs, err := gowfs.NewFileSystem(conf)
```
Now you are ready to communicate with HDFS.

#### Create File
`FileSystem.Create()` creates and store a remote file on the HDFS server.
See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Create
```
ok, err := fs.Create(
    bytes.NewBufferString("Hello webhdfs users!"),
	gowfs.Path{Name:"/remote/file"},
	false,
	0,
	0,
	0700,
	0,
)
```

#### Open HDFS File
Use the `FileSystem.Open()` to open and read a remote file from HDFS.  See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Open
```
data, err := fs.Open(gowfs.Path{Name:"/remote/file"}, 0, 512, 2048)
...
rcvdData, _ := ioutil.ReadAll(data)
fmt.Println(string(rcvdData))

```

#### Append to File
To append to an existing HDFS file, use `FileSystem.Append()`.  See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Append
```
ok, err := fs.Append(
    bytes.NewBufferString("Hello webhdfs users!"),
    gowfs.Path{Name:"/remote/file"}, 4096)
```

#### Rename File
Use `FileSystem.Rename()` to rename HDFS resources. See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Rename
```
ok, err := fs.Rename(gowfs.Path{Name:"/old/name"}, Path{Name:"/new/name"})
```

#### Delete HDFS Resources
To delete an HDFS resource (file/directory), use `FileSystem.Delete()`.  See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.Delete
```go
ok, err := fs.Delete(gowfs.Path{Name:"/remote/file/todelete"}, false)
```

#### File Status
You can get status about an existing HDFS resource using `FileSystem.GetFileStatus()`. See https://godoc.org/github.com/vladimirvivien/gowfs#FileSystem.GetFileStatus

```go
fileStatus, err := fs.GetFileStatus(gowfs.Path{Name:"/remote/file"})
```
gowfs returns a value of type FileStatus which is a struct with info about remote file.
```go
type FileStatus struct {
	AccesTime int64
    BlockSize int64
    Group string
    Length int64
    ModificationTime int64
    Owner string
    PathSuffix string
    Permission string
    Replication int64
    Type string
}
```
You can get a list of file stats using `FileSystem.ListStatus()`.
```go
stats, err := fs.ListStatus(gowfs.Path{Name:"/remote/directory"})
for _, stat := range stats {
    fmt.Println(stat.PathSuffix, stat.Length)
}
```
### FsShell Examples
#### Create the FsShell
To create an FsShell, you need to have an existing instance of FileSystem.
```go
shell := gowfs.FsShell{FileSystem:fs}
```
#### FsShell.Put()
Use the put to upload a local file to an HDFS file system. See https://godoc.org/github.com/vladimirvivien/gowfs#FsShell.PutOne
```go
ok, err := shell.Put("local/file/name", "hdfs/file/path", true)
```
#### FsShell.Get()
Use the Get to retrieve remote HDFS file to local file system. See https://godoc.org/github.com/vladimirvivien/gowfs#FsShell.Get
```go
ok, err := shell.Get("hdfs/file/path", "local/file/name")
```

#### FsShell.AppendToFile()
Append local files to remote HDFS file or directory. See https://godoc.org/github.com/vladimirvivien/gowfs#FsShell.AppendToFile
```go
ok, err := shell.AppendToFile([]string{"local/file/1", "local/file/2"}, "remote/hdfs/path")
```

#### FsShell.Chown()
Change owner for remote file.  See https://godoc.org/github.com/vladimirvivien/gowfs#FsShell.Chown.
```go
ok, err := shell.Chown([]string{"/remote/hdfs/file"}, "owner2")
```

#### FsShell.Chgrp()
Change group of remote HDFS files.  See https://godoc.org/github.com/vladimirvivien/gowfs#FsShell.Chgrp
```go
ok, err := shell.Chgrp([]string{"/remote/hdfs/file"}, "superduper")
```

#### FsShell.Chmod()
Change file mod of remote HDFS files.  See https://godoc.org/github.com/vladimirvivien/gowfs#FsShell.Chmod
```go
ok, err := shell.Chmod([]string{"/remote/hdfs/file/"}, 0744)
```

### Limitations
1. Only "SIMPLE" security mode supported.
2. No support for kerberos (none plan right now)
3. No SSL support yet.

### References
1. WebHDFS API - http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
2. FileSystemShell - http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html#getmerge
