package controller

import (
	"fmt"
	"strings"
)

// The bash trap/termination helpers below reproduce the start script the operator has always
// rendered (they were operator-go v0.12 `pkg/util` bash helpers, removed from the framework in
// v0.13). The Vector shutdown-marker lines the legacy script carried are gone: the v0.13 Vector
// agent runs as a native sidecar and no longer reads the marker file.

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
// its script under. The role containers carry the start script as the command's last element
// (RoleDeclaration.Command), so args stay purely the user's cliOverrides.
var containerCommand = []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}

// roleServerName returns the DolphinScheduler server directory/binary segment for a role:
// master-server, worker-server, api-server, alert-server. Note "alert-server" (the product
// binary), not the container name "alerter-server".
func roleServerName(roleName string) string {
	return fmt.Sprintf("%s-server", roleName)
}

// mainContainerScript is the start script of every role's main container: trap functions,
// background start + wait-for-termination.
func mainContainerScript(roleName string) string {
	return strings.Join([]string{
		commonBashTrapFunctions,
		invokePrepareSignalHandlers,
		fmt.Sprintf("%s/bin/start.sh &", roleServerName(roleName)),
		invokeWaitForTermination,
	}, "\n")
}
