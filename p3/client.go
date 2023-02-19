package p3

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

type P3 struct {
	endpoint        string
	group           string
	accessKeyID     string
	accessKeySecret []byte
	http            *http.Client
}

func New() *P3 {
	return NewWith(
		"http://api.p3.photon.storage:13000",
		os.Getenv("P3_ACCESS_KEY_ID"),
		os.Getenv("P3_ACCESS_KEY_SECRET"),
	)
}

func NewWith(
	endpoint string,
	accessKeyID string,
	accessKeySecret string,
) *P3 {
	return &P3{
		endpoint:        endpoint,
		group:           "gateway/v1",
		accessKeyID:     accessKeyID,
		accessKeySecret: []byte(accessKeySecret),
		http:            &http.Client{},
	}
}

type PutObjectResp struct {
	CID string `json:"cid"`
}

func (p *P3) PutObject(
	bucket string,
	key string,
	data []byte,
) (string, error) {
	r, err := http.NewRequest(
		http.MethodPut,
		fmt.Sprintf("%v/%v/%v", p.endpoint, p.group, key),
		bytes.NewReader(data),
	)
	if err != nil {
		return "", err
	}

	r.Header.Set("x-p3-bucket", bucket)
	h := md5.Sum(data)
	r.Header.Set("x-p3-content-md5", hex.EncodeToString(h[:]))
	r.Header.Set("x-p3-unixtime", fmt.Sprintf("%v", time.Now().Unix()))
	AddAuthHeader(r, bucket, key, p.accessKeyID, p.accessKeySecret)

	resp, err := p.http.Do(r)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("request error with code: %v", resp.StatusCode)
	}

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var res PutObjectResp
	if err := json.Unmarshal(bytes, &res); err != nil {
		return "", err
	}

	return res.CID, nil
}

func (p *P3) GetObject(
	bucket string,
	key string,
) ([]byte, error) {
	r, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("%v/%v/%v", p.endpoint, p.group, key),
		nil,
	)
	if err != nil {
		return nil, err
	}

	r.Header.Set("x-p3-bucket", bucket)
	r.Header.Set("x-p3-unixtime", fmt.Sprintf("%v", time.Now().Unix()))
	AddAuthHeader(r, bucket, key, p.accessKeyID, p.accessKeySecret)

	resp, err := p.http.Do(r)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request error with code: %v", resp.StatusCode)
	}

	return ioutil.ReadAll(resp.Body)
}

func (p *P3) GetObjectByCID(cid string) ([]byte, error) {
	r, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("%v/%v/%v?is_cid=1", p.endpoint, p.group, cid),
		nil,
	)
	if err != nil {
		return nil, err
	}

	r.Header.Set("x-p3-unixtime", fmt.Sprintf("%v", time.Now().Unix()))
	AddAuthHeader(r, "", cid, p.accessKeyID, p.accessKeySecret)

	resp, err := p.http.Do(r)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request error with code: %v", resp.StatusCode)
	}

	return ioutil.ReadAll(resp.Body)
}
