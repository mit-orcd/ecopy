#!/bin/sh
#
# Minimal stand-in for `ssh` used by the ecopy test harness.
#
# The ecopy client invokes ssh as:  ssh [-p PORT] [-o OPT] HOST "REMOTE_CMD"
# where REMOTE_CMD is always the final argument. This script ignores the ssh
# options and the host, and simply runs REMOTE_CMD locally through the shell,
# wiring stdin/stdout straight through. That lets the harness exercise the full
# protocol + `ecopy --server` peer without a real SSH connection or keys.
#
# Point ecopy at it with:  ECOPY_SSH=tests/fake_ssh.sh
#
last=""
for a in "$@"; do
    last="$a"
done
exec /bin/sh -c "$last"
