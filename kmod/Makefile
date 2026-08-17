# SPDX-License-Identifier: GPL-2.0-only
#
# Makefile for the boxfs filesystem
#

ifneq ($(KERNELRELEASE),)

ifdef CONFIG_BOXFS_FS
obj-$(CONFIG_BOXFS_FS) += boxfs.o
else
obj-m += boxfs.o
endif

boxfs-y := super.o rust_helpers.o boxfs_rust.o

BOXFS_RUST_SRC := $(src)/rust

ifeq ($(SRCARCH),x86)
BOXFS_RUST_TARGET := x86_64-unknown-none
else
$(error boxfs: no freestanding Rust target is wired up for SRCARCH=$(SRCARCH))
endif

BOXFS_RUST_LIB := $(BOXFS_RUST_SRC)/target/$(BOXFS_RUST_TARGET)/release/libboxfs_kmod.a

# rustup ships core and alloc as position-independent rlibs, so -Zbuild-std
# rebuilds them under the relocation model the module loader accepts.
BOXFS_CARGO ?= cargo +nightly
BOXFS_CARGO_FLAGS := build --release --no-default-features \
		     --target $(BOXFS_RUST_TARGET) -Zbuild-std=core,alloc

# Only the C-ABI entry points seed the partial link, so the unreachable half of
# compiler_builtins (soft-float, libm) stays out of the module.
BOXFS_RUST_EXPORTS := \
	boxfs_rust_fill_super boxfs_rust_put_super boxfs_rust_statfs \
	boxfs_rust_lookup boxfs_rust_iterate_dir boxfs_rust_read \
	boxfs_rust_getattr boxfs_rust_readlink boxfs_rust_readahead \
	boxfs_rust_getxattr boxfs_rust_listxattr

quiet_cmd_boxfs_rust = CARGO   $@
      cmd_boxfs_rust = cd $(BOXFS_RUST_SRC) && \
		       CARGO_NET_GIT_FETCH_WITH_CLI=true \
		       $(BOXFS_CARGO) $(BOXFS_CARGO_FLAGS) && \
		       $(LD) -r --gc-sections -o $(abspath $@) \
			     $(addprefix -u,$(BOXFS_RUST_EXPORTS)) \
			     $(abspath $(BOXFS_RUST_LIB)) && \
		       $(OBJCOPY) --remove-section=.eh_frame $(abspath $@)

# if_changed ignores FORCE, so the crate sources have to be real prerequisites
# for an edit to reach the module.
BOXFS_RUST_DEPS := \
	$(wildcard $(BOXFS_RUST_SRC)/*.rs) \
	$(wildcard $(BOXFS_RUST_SRC)/Cargo.toml) \
	$(wildcard $(BOXFS_RUST_SRC)/Cargo.lock) \
	$(wildcard $(BOXFS_RUST_SRC)/.cargo/config.toml) \
	$(wildcard $(src)/../crates/fst/src/*.rs) \
	$(wildcard $(src)/../crates/fst/Cargo.toml)

targets += boxfs_rust.o

$(obj)/boxfs_rust.o: $(BOXFS_RUST_DEPS) FORCE
	$(call if_changed,boxfs_rust)

else

KDIR ?= /lib/modules/$(shell uname -r)/build
PWD := $(shell pwd)

default:
	$(MAKE) -C $(KDIR) M=$(PWD) modules

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
	rm -rf $(PWD)/rust/target

install:
	$(MAKE) -C $(KDIR) M=$(PWD) modules_install

.PHONY: default clean install

endif
