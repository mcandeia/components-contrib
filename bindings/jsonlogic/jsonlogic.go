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

package jsonlogic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/diegoholiveira/jsonlogic"
)

const (
	// EvaluateOperation is the operation to evaluate a jsonlogic rule.
	EvaluateOperation = "evaluate"
)

var (
	// ErrJsonLogicExpressionMissing is returned when no jsonlogic expression is provided.
	ErrJsonLogicExpressionMissing = errors.New("jsonlogic expression is missing")
)

// JsonLogicRule is a defined JsonLogic rule using component metadata
type EvaluationRequest struct {
	// Data is the data related to the evaluation.
	Data any
	// Expression is the JsonLogic raw expression.
	Expression any
}

// jsonLogicOutput is the jsonLogicOutput output binding to evaluate jsonLogicOutput expressions.
type jsonLogicOutput struct {
	logger logger.Logger
}

// Init performs metadata parsing.
func (jl *jsonLogicOutput) Init(metadata bindings.Metadata) error {
	return nil
}

// evaluate gets the data and the logic expression and return the bindings invoke response after the rule evaluation against the received data.
func (jl *jsonLogicOutput) evaluate(evalReq *EvaluationRequest) (*bindings.InvokeResponse, error) {
	rawJSON, err := jsonlogic.ApplyInterface(evalReq.Expression, evalReq.Data)

	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(rawJSON)
	if err != nil {
		return nil, err
	}

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

// Invoke is called for output bindings.
func (jl *jsonLogicOutput) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	data, err := strconv.Unquote(string(req.Data))
	if err != nil {
		return nil, err
	}
	var evalReq *EvaluationRequest

	if err := json.Unmarshal([]byte(data), &evalReq); err != nil {
		return nil, err
	}

	switch req.Operation {
	case EvaluateOperation:
		return jl.evaluate(evalReq)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}

// Operations enumerates supported binding operations.
func (jl *jsonLogicOutput) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{EvaluateOperation}
}

// NewJsonLogic returns a new JsonLogic conditional output binding.
func NewJsonLogic(logger logger.Logger) bindings.OutputBinding {
	return &jsonLogicOutput{logger: logger}
}
