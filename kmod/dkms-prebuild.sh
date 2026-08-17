#!/bin/sh
# SPDX-License-Identifier: GPL-2.0-only
#
# Link the box checkout's crates next to the DKMS build tree so the Rust
# core's path dependencies resolve. Run by DKMS from the build directory.

set -eu

: "${BOXFS_CHECKOUT:=}"

if [ -z "$BOXFS_CHECKOUT" ]; then
	source_dir="/usr/src/boxfs-0.1.0"
	if [ -e "$source_dir" ]; then
		BOXFS_CHECKOUT=$(dirname "$(readlink -f "$source_dir")")
	fi
fi

if [ -z "$BOXFS_CHECKOUT" ] || [ ! -d "$BOXFS_CHECKOUT/crates/fst" ]; then
	echo "boxfs: cannot find the box checkout." >&2
	echo "boxfs: symlink /usr/src/boxfs-0.1.0 -> <checkout>/kmod, or set" >&2
	echo "boxfs: BOXFS_CHECKOUT=<checkout> in the environment." >&2
	exit 1
fi

for entry in Cargo.toml Cargo.lock build.rs src crates cli; do
	if [ -e "$BOXFS_CHECKOUT/$entry" ]; then
		ln -sfn "$BOXFS_CHECKOUT/$entry" "../$entry"
	fi
done
