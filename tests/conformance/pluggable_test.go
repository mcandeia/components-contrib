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

package conformance

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	confState "github.com/dapr/components-contrib/tests/conformance/state"
	"github.com/dapr/components-contrib/tests/conformance/utils"

	"github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/require"
)

var l = logger.NewLogger("dapr-conformance-tests")

func TestPluggableConformance(t *testing.T) {
	t.Run("state", func(t *testing.T) {
		socket := "/tmp/socket.sock"
		if soc, ok := os.LookupEnv("DAPR_CONFORMANCE_COMPONENT_SOCKET"); ok {
			socket = soc
		}

		var componentMetadata map[string]string
		if metadataPath, ok := os.LookupEnv("DAPR_CONFORMANCE_COMPONENT_METADATA_FILE"); ok {
			f, err := os.ReadFile(metadataPath)
			require.NoError(t, err)
			json.Unmarshal(f, &componentMetadata)
		}

		operations, allOperations := []string{}, true
		if operationsList, ok := os.LookupEnv("DAPR_CONFORMANCE_COMPONENT_OPERATIONS"); ok {
			allOperations = false
			operations = strings.Split(operationsList, ",")
		}
		stateStore := state.NewGRPCStateStore(l, func(_ string) string {
			return socket
		})

		operationMap := make(map[string]struct{})

		for _, op := range operations {
			operationMap[op] = struct{}{}
		}
		confState.ConformanceTests(t, componentMetadata, stateStore, confState.TestConfig{
			CommonConfig: utils.CommonConfig{
				AllOperations: allOperations,
				Operations:    operationMap,
			},
		})
	})
}
