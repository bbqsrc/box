# SPDX-License-Identifier: GPL-2.0-only
#
# Makefile for the boxfs filesystem
#

# For out-of-tree builds
ifneq ($(KERNELRELEASE),)

obj-$(CONFIG_BOXFS_FS) += boxfs.o

boxfs-y := super.o rust_helpers.o
boxfs-y += rust/boxfs_rust.o

# Rust flags
RUST_FLAGS_boxfs_rust.o := --extern box_format_core

else

KDIR ?= /lib/modules/$(shell uname -r)/build
PWD := $(shell pwd)

default:
	$(MAKE) -C $(KDIR) M=$(PWD) modules

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean

install:
	$(MAKE) -C $(KDIR) M=$(PWD) modules_install

.PHONY: default clean install

endif
