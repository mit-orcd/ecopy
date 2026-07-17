#!/bin/sh
#
# Bootstrap-exercising stand-in for `ssh`, used by the ecopy test harness.
#
# Like tests/fake_ssh.sh it runs the final argument (the remote command) locally
# through the shell, but it *refuses* the bare `ecopy ...` command with exit 127
# to simulate a remote host that has no ecopy on PATH ("command not found").
# Everything else runs normally, so the client's bootstrap path is exercised:
#   - `uname -sm`                      -> platform probe (matches, runs locally)
#   - `cat > /tmp/.ecopy-boot-*.tmp..` -> binary upload (real ecopy copied in)
#   - `/tmp/.ecopy-boot-* --server ..` -> the uploaded binary runs as the peer
#
# Point ecopy at it with:  ECOPY_SSH=tests/fake_ssh_bootstrap.sh  and do NOT set
# ECOPY_REMOTE_CMD, so the client starts from the bare `ecopy` command.
last=""
for a in "$@"; do
    last="$a"
done

case "$last" in
    ecopy\ *|ecopy)
        echo "bash: line 1: ecopy: command not found" >&2
        exit 127
        ;;
    *)
        exec /bin/sh -c "$last"
        ;;
esac
