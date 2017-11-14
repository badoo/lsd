package gowfs

import "fmt"
import "net/url"

// Root level struct for data JSON data from WebHDFS.
type HdfsJsonData struct {
	Boolean         bool
	FileStatus      FileStatus
	FileStatuses    FileStatuses
	FileChecksum    FileChecksum
	ContentSummary  ContentSummary
	Token           Token
	Tokens          Tokens
	Long            int64
	RemoteException RemoteException
}

// Represents a remote webHDFS path
type Path struct {
	Name       string  // Relative path representation (/root/leaf)
	RefererUrl url.URL // URL related to path (http://server:port/root/leaf)
}

// Represents HDFS FileStatus (FileSystem.getStatus())
// See http://hadoop.apache.org/docs/r2.2.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileStatus_JSON_Schema
//
// Example:
// {
//   "FileStatus":
//   {
//     "accessTime"      : 0, 				// integer
//     "blockSize"       : 0, 				// integer
//     "group"           : "grp",			// string
//     "length"          : 0,             	// integer - zero for directories
//     "modificationTime": 1320173277227,	// integer
//     "owner"           : "webuser",		// string
//     "pathSuffix"      : "",				// string
//     "permission"      : "777",			// string
//     "replication"     : 0,				// integer
//     "type"            : "DIRECTORY"    	// string - enum {FILE, DIRECTORY, SYMLINK}
//   }
// }
type FileStatus struct {
	AccesTime        int64
	BlockSize        int64
	Group            string
	Length           int64
	ModificationTime int64
	Owner            string
	PathSuffix       string
	Permission       string
	Replication      int64
	Type             string
}

// Container type for multiple FileStatus for directory, etc
// (see HDFS FileSystem.listStatus())
// NOTE: the confusing naming and Plurality is to match WebHDFS schema.
type FileStatuses struct {
	FileStatus []FileStatus
}

// 	Type for HDFS FileSystem content summary (FileSystem.getContentSummary())
// 	See http://hadoop.apache.org/docs/r2.2.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#ContentSummary_JSON_Schema
//
// Example:
// {
//   "ContentSummary":
//   {
//     "directoryCount": 2,
//     "fileCount"     : 1,
//     "length"        : 24930,
//     "quota"         : -1,
//     "spaceConsumed" : 24930,
//     "spaceQuota"    : -1
//   }
// }
type ContentSummary struct {
	DirectoryCount int64
	FileCount      int64
	Length         int64
	Quota          int64
	SpaceConsumed  int64
	SpaceQuota     int64
}

// 	Type for HDFS FileSystem.getFileChecksum()
// 	See http://hadoop.apache.org/docs/r2.2.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#FileChecksum_JSON_Schema
//
// Example:
// {
//   "FileChecksum":
//   {
//     "algorithm": "MD5-of-1MD5-of-512CRC32",
//     "bytes"    : "eadb10de24aa315748930df6e185c0d ...",
//     "length"   : 28
//   }
// }
type FileChecksum struct {
	Algorithm string
	Bytes     string
	Length    int64
}

// Type for HDFS FileSystem delegation token (FileSystem.getDelegationToken())
// See http://hadoop.apache.org/docs/r2.2.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Token_JSON_Schema

// Example:
// {
//   "Token":
//   {
//     "urlString": "JQAIaG9y..."
//   }
// }
type Token struct {
	UrlString string
}

/*
	Container type for Token
*/
type Tokens struct {
	Token []Token
}

// Type for returning WebHDFS error/exceptions.
// See http://hadoop.apache.org/docs/r2.2.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#RemoteException_JSON_schema

// Example:
// {
//   "RemoteException":
//   {
//     "exception"    : "FileNotFoundException",
//     "javaClassName": "java.io.FileNotFoundException",
//     "message"      : "File does not exist: /foo/a.patch"
//   }
// }
type RemoteException struct {
	Exception     string
	JavaClassName string
	Message       string
}

// Implementation of error type.  Returns string representation of RemoteException.
func (re RemoteException) Error() string {
	return fmt.Sprintf("RemoteException: %v [%v]\n[%v]\n", re.Exception, re.JavaClassName, re.Message)
}
