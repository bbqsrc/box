#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-2.0-only
#
# Load boxfs, mount an archive read-only, and diff the mounted tree against
# what `box extract` produces. Run as root.

set -uo pipefail

usage() {
	cat >&2 <<'EOF'
usage: sudo ./smoke.sh ARCHIVE.box

environment:
  BOXFS_KO   path to boxfs.ko            (default: <script dir>/boxfs.ko)
  BOX        path to the box CLI         (default: searched, then $PATH)
  TMPDIR     scratch for the extraction  (needs room for the whole archive)
EOF
	exit 2
}

[ $# -eq 1 ] || usage
ARCHIVE=$1

if [ "$(id -u)" -ne 0 ]; then
	echo "smoke.sh: must run as root (insmod/mount/losetup)" >&2
	exit 2
fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
KO=${BOXFS_KO:-$SCRIPT_DIR/boxfs.ko}

if [ -z "${BOX:-}" ]; then
	for candidate in \
		"$SCRIPT_DIR/../target/release/box" \
		"$SCRIPT_DIR/../target/debug/box" \
		"$(command -v box 2>/dev/null)"; do
		if [ -n "$candidate" ] && [ -x "$candidate" ]; then
			BOX=$candidate
			break
		fi
	done
fi

for required in "$ARCHIVE" "$KO" "${BOX:-}"; do
	if [ -z "$required" ] || [ ! -e "$required" ]; then
		echo "smoke.sh: missing ${required:-box CLI}; see usage" >&2
		usage
	fi
done

WORK=$(mktemp -d "${TMPDIR:-/tmp}/boxfs-smoke.XXXXXX") || exit 1
MNT=$WORK/mnt
EXTRACT=$WORK/extract
LOOP=
MOUNTED=
INSERTED=
FAILURES=0

mkdir -p "$MNT" "$EXTRACT"

cleanup() {
	local status=$?
	set +e
	if [ -n "$MOUNTED" ]; then
		umount "$MNT" >/dev/null 2>&1 || umount -l "$MNT" >/dev/null 2>&1
	fi
	if [ -n "$LOOP" ]; then
		losetup -d "$LOOP" >/dev/null 2>&1
	fi
	if [ -n "$INSERTED" ]; then
		rmmod boxfs >/dev/null 2>&1
	fi
	rm -rf -- "$WORK"
	exit $status
}
trap cleanup EXIT INT TERM

now_ms() { echo $(($(date +%s%N) / 1000000)); }

STEP_START=0
step() {
	STEP_START=$(now_ms)
	printf '==> %s\n' "$1"
}
step_done() {
	printf '    %s ms\n' "$(( $(now_ms) - STEP_START ))"
}

fail() {
	printf 'FAIL: %s\n' "$1" >&2
	FAILURES=$((FAILURES + 1))
}

# ---------------------------------------------------------------------------

printf 'archive: %s (%s bytes)\n' "$ARCHIVE" "$(stat -c %s "$ARCHIVE")"
printf 'module:  %s\n' "$KO"
printf 'box:     %s\n' "$BOX"
printf 'scratch: %s\n\n' "$WORK"

# A loop device exposes whole 512-byte sectors, so an archive whose length is
# not a multiple of 512 would lose the tail of its trailer.
IMAGE=$ARCHIVE
SIZE=$(stat -c %s "$ARCHIVE")
if [ $((SIZE % 512)) -ne 0 ]; then
	step "padding archive to a sector boundary ($((512 - SIZE % 512)) bytes)"
	cp --reflink=auto -- "$ARCHIVE" "$WORK/image.box" || exit 1
	truncate -s $(((SIZE + 511) / 512 * 512)) "$WORK/image.box" || exit 1
	IMAGE=$WORK/image.box
	step_done
fi

step "insmod $(basename "$KO")"
if ! insmod "$KO"; then
	echo "smoke.sh: insmod failed; see dmesg" >&2
	exit 1
fi
INSERTED=1
step_done

step "losetup"
LOOP=$(losetup --find --show --read-only "$IMAGE") || exit 1
printf '    %s\n' "$LOOP"
step_done

step "mount -t boxfs -o ro"
if ! mount -t boxfs -o ro "$LOOP" "$MNT"; then
	echo "smoke.sh: mount failed; see dmesg" >&2
	exit 1
fi
MOUNTED=1
step_done

step "box extract (reference tree)"
if ! "$BOX" extract "$ARCHIVE" -o "$EXTRACT" --xattrs \
	--allow-external-symlinks --allow-escapes --quiet; then
	fail "box extract"
fi
step_done

step "diff -r --no-dereference (contents and symlink targets)"
if ! diff -r --no-dereference "$EXTRACT" "$MNT" > "$WORK/diff.out" 2>&1; then
	fail "mounted tree differs from the extracted tree"
	head -n 40 "$WORK/diff.out" >&2
fi
step_done

step "metadata (type, mode, size)"
listing() {
	(
		cd -- "$1" || exit 1
		find . -mindepth 1 \
			\( -type f -printf 'f %m %s %p\n' \) -o \
			\( -type d -printf 'd %m %p\n' \) -o \
			\( -type l -printf 'l %m %p\n' \) |
			LC_ALL=C sort
	)
}
listing "$EXTRACT" > "$WORK/extract.list"
listing "$MNT" > "$WORK/mnt.list"
if ! diff -u "$WORK/extract.list" "$WORK/mnt.list" > "$WORK/meta.out" 2>&1; then
	fail "type/mode/size listing differs"
	head -n 40 "$WORK/meta.out" >&2
fi
step_done

step "readdir sanity (. and .. present)"
if ! ls -a "$MNT" | grep -qx '\.\.'; then
	fail "mount root does not list .."
fi
step_done

step "statfs"
if ! stat -f "$MNT" > "$WORK/statfs.out" 2>&1; then
	fail "statfs on the mount"
else
	sed 's/^/    /' "$WORK/statfs.out"
fi
step_done

step "re-read one large file (block cache path)"
BIGGEST=$(find "$MNT" -type f -printf '%s %p\n' | LC_ALL=C sort -rn | head -n 1 | cut -d' ' -f2-)
if [ -n "$BIGGEST" ]; then
	printf '    %s\n' "$BIGGEST"
	REL=${BIGGEST#"$MNT"/}
	for _ in 1 2; do
		if ! cmp -s "$BIGGEST" "$EXTRACT/$REL"; then
			fail "re-read of $REL differs"
			break
		fi
	done
fi
step_done

step "umount"
if ! umount "$MNT"; then
	fail "umount"
else
	MOUNTED=
fi
step_done

step "losetup -d"
if losetup -d "$LOOP"; then
	LOOP=
else
	fail "losetup -d"
fi
step_done

step "rmmod boxfs"
if rmmod boxfs; then
	INSERTED=
else
	fail "rmmod"
fi
step_done

echo
if [ "$FAILURES" -eq 0 ]; then
	echo "PASS"
	exit 0
fi
echo "FAIL ($FAILURES check(s))"
exit 1
