/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package middleware

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/http"
)

type netHttpCtrl struct {
	w        http.ResponseWriter
	r        *http.Request
	next     http.HandlerFunc
	lazyBody []byte
}

// do not support concurrency
func (f *netHttpCtrl) GetRequestBody() ([]byte, error) {
	if f.lazyBody != nil {
		return f.lazyBody, nil
	}
	b, err := ioutil.ReadAll(f.r.Body)
	if err != nil {
		return nil, err
	}
	f.lazyBody = b

	return f.lazyBody, nil
}

func (f *netHttpCtrl) SetRequestBody(body []byte) {
	f.r.Body = io.NopCloser(bytes.NewBuffer(body))
}

func (f *netHttpCtrl) SetResponseBody(body []byte) {
	f.w.Write(body)
}

func (f *netHttpCtrl) GetRequestHeaders() map[string][]string {
	return f.r.Header
}

func (f *netHttpCtrl) SetRequestHeaders(headers map[string][]string) {
	for headerKey, headerValue := range headers {
		for _, value := range headerValue {
			f.r.Header.Add(headerKey, value)
		}
	}
}

func (f *netHttpCtrl) GetResponseHeaders() map[string][]string {
	return f.w.Header()
}

func (f *netHttpCtrl) SetResponseHeaders(headers map[string][]string) {
	respHeaders := f.w.Header()
	for headerKey, headerValue := range headers {
		for _, value := range headerValue {
			respHeaders.Set(headerKey, value)
		}
	}
}

func (f *netHttpCtrl) SetStatusCode(code int) {
	f.w.WriteHeader(code)
}

func (f *netHttpCtrl) SetRequestURI(uri string) {
	f.r.RequestURI = uri
}

func (f *netHttpCtrl) GetRequestURI() string {
	return f.r.RequestURI
}

func (f *netHttpCtrl) GetMethod() string {
	return f.r.Method
}

func (f *netHttpCtrl) SetMethod(method string) {
	f.r.Method = method
}

func (f *netHttpCtrl) GetHeaderValue(key string) []string {
	return f.r.Header[key]
}

func (f *netHttpCtrl) SetHeaderValue(key string, values []string) {
	for _, value := range values {
		f.r.Header.Set(key, value)
	}
}

// Next execute the downstream middleware
func (f *netHttpCtrl) Next() {
	f.next(f.w, f.r)
}

func (f *netHttpCtrl) Error(msg string, code int) {
	f.SetStatusCode(code)
	f.w.Write([]byte(msg))
}

func (f *netHttpCtrl) Context() context.Context {
	return context.TODO()
}
