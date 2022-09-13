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
	"context"

	"github.com/valyala/fasthttp"
)

type fastHttpCtrl struct {
	next fasthttp.RequestHandler
	ctx  *fasthttp.RequestCtx
}

func (f *fastHttpCtrl) GetRequestBody() ([]byte, error) {
	return f.ctx.Request.Body(), nil
}

func (f *fastHttpCtrl) SetRequestBody(body []byte) {
	f.ctx.Request.SetBody(body)
}

func (f *fastHttpCtrl) SetResponseBody(body []byte) {
	f.ctx.Response.SetBody(body)
}

func (f *fastHttpCtrl) GetRequestHeaders() map[string][]string {
	headers := make(map[string][]string)

	f.ctx.Request.Header.VisitAll(func(key, value []byte) {
		headers[string(key[:])] = []string{string(value[:])} // fixme
	})
	return headers
}

func (f *fastHttpCtrl) SetRequestHeaders(headers map[string][]string) {
	for headerKey, headerValue := range headers {
		for _, value := range headerValue {
			f.ctx.Request.Header.Add(headerKey, value)
		}
	}
}

func (f *fastHttpCtrl) GetResponseHeaders() map[string][]string {
	headers := make(map[string][]string)

	f.ctx.Response.Header.VisitAll(func(key, value []byte) {
		headers[string(key[:])] = []string{string(value[:])}
	})
	return headers
}

func (f *fastHttpCtrl) SetResponseHeaders(headers map[string][]string) {
	for headerKey, headerValue := range headers {
		for _, value := range headerValue {
			f.ctx.Response.Header.Add(headerKey, value)
		}
	}
}

func (f *fastHttpCtrl) SetStatusCode(code int) {
	f.ctx.SetStatusCode(code)
}

func (f *fastHttpCtrl) SetRequestURI(uri string) {
	f.ctx.Request.SetRequestURI(uri)
}

func (f *fastHttpCtrl) GetRequestURI() string {
	return string(f.ctx.Request.RequestURI())
}

func (f *fastHttpCtrl) GetMethod() string {
	return string(f.ctx.Request.Header.Method())
}

func (f *fastHttpCtrl) SetMethod(method string) {
	f.ctx.Request.Header.SetMethod(method)
}

func (f *fastHttpCtrl) GetHeaderValue(key string) string {
	return string(f.ctx.Request.Header.Peek(key))
}

func (f *fastHttpCtrl) SetHeaderValue(key, value string) {
	f.ctx.Request.Header.Set(key, value)
}

// Next execute the downstream middleware
func (f *fastHttpCtrl) Next() {
	f.next(f.ctx)
}

func (f *fastHttpCtrl) Error(msg string, code int) {
	f.Error(msg, code)
}

func (f *fastHttpCtrl) Context() context.Context {
	return context.TODO()
}
