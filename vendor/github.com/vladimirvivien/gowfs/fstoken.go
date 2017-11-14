package gowfs

import "fmt"
import "net/http"

func (fs *FileSystem) GetDelegationToken(renewer string) (Token, error) {
	params := map[string]string{"op": OP_GETDELEGATIONTOKEN, "renewer": renewer}

	u, err := buildRequestUrl(fs.Config, nil, &params)
	if err != nil {
		return Token{}, err
	}

	req, _ := http.NewRequest("GET", u.String(), nil)
	hdfsData, err := requestHdfsData(fs.client, *req)
	if err != nil {
		return Token{}, err
	}

	return hdfsData.Token, nil
}

func (fs *FileSystem) GetDelegationTokens(renewer string) ([]Token, error) {
	params := map[string]string{"op": OP_GETDELEGATIONTOKENS, "renewer": renewer}

	u, err := buildRequestUrl(fs.Config, nil, &params)
	if err != nil {
		return nil, err
	}

	req, _ := http.NewRequest("GET", u.String(), nil)
	hdfsData, err := requestHdfsData(fs.client, *req)
	if err != nil {
		return nil, err
	}

	return hdfsData.Tokens.Token, nil
}

func (fs *FileSystem) RenewDelegationToken(token string) (int64, error) {
	params := map[string]string{"op": OP_RENEWDELEGATIONTOKEN, "token": token}

	u, err := buildRequestUrl(fs.Config, nil, &params)
	if err != nil {
		return -1, err
	}

	req, _ := http.NewRequest("PUT", u.String(), nil)
	hdfsData, err := requestHdfsData(fs.client, *req)
	if err != nil {
		return -1, err
	}

	return hdfsData.Long, nil

}

func (fs *FileSystem) CancelDelegationToken(token string) (bool, error) {
	params := map[string]string{"op": OP_CANCELDELEGATIONTOKEN, "token": token}

	u, err := buildRequestUrl(fs.Config, nil, &params)
	if err != nil {
		return false, err
	}

	req, _ := http.NewRequest("PUT", u.String(), nil)
	rsp, err := fs.client.Do(req)
	if err != nil {
		return false, err
	}
	if rsp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("SetPermission() - server returned unexpected status, token not cancelled.")
	}

	return true, nil
}
