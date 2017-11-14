/*
gowfs is Go bindings for the Hadoop HDFS over its WebHDFS interface.
gowfs uses JSON marshalling to expose typed values from HDFS.
See https://github.com/vladimirvivien/gowfs.
*/
package gowfs

import "encoding/json"
import "net"
import "net/http"
import "net/url"
import "io/ioutil"

const (
	OP_OPEN                  = "OPEN"
	OP_CREATE                = "CREATE"
	OP_APPEND                = "APPEND"
	OP_CONCAT                = "CONCAT"
	OP_RENAME                = "RENAME"
	OP_DELETE                = "DELETE"
	OP_SETPERMISSION         = "SETPERMISSION"
	OP_SETOWNER              = "SETOWNER"
	OP_SETREPLICATION        = "SETREPLICATION"
	OP_SETTIMES              = "SETTIMES"
	OP_MKDIRS                = "MKDIRS"
	OP_CREATESYMLINK         = "CREATESYMLINK"
	OP_LISTSTATUS            = "LISTSTATUS"
	OP_GETFILESTATUS         = "GETFILESTATUS"
	OP_GETCONTENTSUMMARY     = "GETCONTENTSUMMARY"
	OP_GETFILECHECKSUM       = "GETFILECHECKSUM"
	OP_GETDELEGATIONTOKEN    = "GETDELEGATIONTOKEN"
	OP_GETDELEGATIONTOKENS   = "GETDELEGATIONTOKENS"
	OP_RENEWDELEGATIONTOKEN  = "RENEWDELEGATIONTOKEN"
	OP_CANCELDELEGATIONTOKEN = "CANCELDELEGATIONTOKEN"
)

// Hack for in-lining multi-value functions
func µ(v ...interface{}) []interface{} {
	return v
}

// This type maps fields and functions to HDFS's FileSystem class.
type FileSystem struct {
	Config    Configuration
	client    http.Client
	transport *http.Transport
}

func NewFileSystem(conf Configuration) (*FileSystem, error) {
	fs := &FileSystem{
		Config: conf,
	}
	fs.transport = &http.Transport{
		Dial: func(netw, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(netw, addr, conf.ConnectionTimeout)
			if err != nil {
				return nil, err
			}

			return c, nil
		},
		MaxIdleConnsPerHost:   conf.MaxIdleConnsPerHost,
		ResponseHeaderTimeout: conf.ResponseHeaderTimeout,
	}
	fs.client = http.Client{
		Transport: fs.transport,
	}
	return fs, nil
}

// Builds the canonical URL used for remote request
func buildRequestUrl(conf Configuration, p *Path, params *map[string]string) (*url.URL, error) {
	u, err := conf.GetNameNodeUrl()
	if err != nil {
		return nil, err
	}

	//prepare URL - add Path and "op" to URL
	if p != nil {
		if p.Name[0] == '/' {
			u.Path = u.Path + p.Name
		} else {
			u.Path = u.Path + "/" + p.Name
		}
	}

	q := u.Query()

	// attach params
	if params != nil {
		for key, val := range *params {
			q.Add(key, val)
		}
	}
	u.RawQuery = q.Encode()

	return u, nil
}

func makeHdfsData(data []byte) (HdfsJsonData, error) {
	if len(data) == 0 || data == nil {
		return HdfsJsonData{}, nil
	}
	var jsonData HdfsJsonData
	jsonErr := json.Unmarshal(data, &jsonData)

	if jsonErr != nil {
		return HdfsJsonData{}, jsonErr
	}

	// check for remote exception
	if jsonData.RemoteException.Exception != "" {
		return HdfsJsonData{}, jsonData.RemoteException
	}

	return jsonData, nil

}

func responseToHdfsData(rsp *http.Response) (HdfsJsonData, error) {
	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return HdfsJsonData{}, err
	}
	return makeHdfsData(body)
}

func requestHdfsData(client http.Client, req http.Request) (HdfsJsonData, error) {
	rsp, err := client.Do(&req)
	if err != nil {
		return HdfsJsonData{}, err
	}
	defer rsp.Body.Close()
	hdfsData, err := responseToHdfsData(rsp)
	return hdfsData, err
}
