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
)

// HTTPHeaders is the http request headers.
type HTTPHeaders struct {
	Headers map[string]string
	URI     string
	Method  string
}

// MiddlewareCtrl is the middleware interface controller.
type MiddlewareCtrl interface {
	GetRequestBody() ([]byte, error)
	SetRequestBody(body []byte)
	SetResponseBody(body []byte)
	GetRequestHeaders() map[string][]string
	SetRequestHeaders(headers map[string][]string)
	GetResponseHeaders() map[string][]string
	SetResponseHeaders(headers map[string][]string)
	SetStatusCode(code int)
	SetRequestURI(uri string)
	GetRequestURI() string
	GetMethod() string
	SetMethod(method string)
	GetHeaderValue(key string) string
	SetHeaderValue(key, value string)
	Error(msg string, code int)
	// Next execute the downstream middleware
	Next()

	Context() context.Context
}
