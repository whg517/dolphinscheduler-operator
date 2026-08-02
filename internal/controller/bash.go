package controller

import (
	"fmt"
	"strings"

	opgoconstant "github.com/zncdatadev/operator-go/pkg/constant"
)

// The bash trap/termination helpers below reproduce, byte for byte, the start script the
// operator has always rendered (they were operator-go v0.12 `pkg/util` bash helpers, removed
// from the framework in v0.13). The pods' args are part of the rendered-YAML parity contract,
// so they are kept verbatim here.

// vectorShutdownDir/vectorShutdownFile are the Vector shutdown marker the legacy script
// touches on exit. The v0.13 Vector agent runs as a native sidecar and no longer reads the
// marker, but the script lines stay for byte parity of the container args.
const (
	vectorLogSubDir    = "_vector/"
	vectorShutdownFile = "shutdown"
)

const (
	invokePrepareSignalHandlers = "prepare_signal_handlers"
	invokeWaitForTermination    = "wait_for_termination $!"
)

const commonBashTrapFunctions = `prepare_signal_handlers()
{
    unset term_child_pid
    unset term_kill_needed
    trap 'handle_term_signal' TERM
}

handle_term_signal()
{
    if [ "${term_child_pid}" ]; then
        kill -TERM "${term_child_pid}" 2>/dev/null
    else
        term_kill_needed="yes"
    fi
}

wait_for_termination()
{
    set +e
    term_child_pid=$1
    if [[ -v term_kill_needed ]]; then
        kill -TERM "${term_child_pid}" 2>/dev/null
    fi
    wait ${term_child_pid} 2>/dev/null
    trap - TERM
    wait ${term_child_pid} 2>/dev/null
    set -e
}`

// containerCommand is the shell every DolphinScheduler container (and the DB-init Job) runs
// its script under.
var containerCommand = []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}

// removeVectorShutdownFileCommand removes the marker file before the product starts.
func removeVectorShutdownFileCommand() string {
	return fmt.Sprintf("rm -f %s%s%s", opgoconstant.KubedoopLogDir, vectorLogSubDir, vectorShutdownFile)
}

// createVectorShutdownFileCommand creates the marker file after the product terminates.
func createVectorShutdownFileCommand() string {
	return fmt.Sprintf("mkdir -p %s%s && touch %s%s%s",
		opgoconstant.KubedoopLogDir, vectorLogSubDir, opgoconstant.KubedoopLogDir, vectorLogSubDir, vectorShutdownFile)
}

// roleServerName returns the DolphinScheduler server directory/binary segment for a role:
// master-server, worker-server, api-server, alert-server. Note "alert-server" (the product
// binary), not the container name "alerter-server".
func roleServerName(roleName string) string {
	return fmt.Sprintf("%s-server", roleName)
}

// mainContainerScript is the start script of every role's main container: trap functions,
// Vector marker handling, background start + wait-for-termination. Identical to the legacy
// rendering (one block; the historical duplicated block of the api role is deliberately gone).
func mainContainerScript(roleName string) string {
	return strings.Join([]string{
		commonBashTrapFunctions,
		removeVectorShutdownFileCommand(),
		invokePrepareSignalHandlers,
		fmt.Sprintf("%s/bin/start.sh &", roleServerName(roleName)),
		invokeWaitForTermination,
		createVectorShutdownFileCommand(),
	}, "\n")
}
