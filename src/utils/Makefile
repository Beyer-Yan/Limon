SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../../../)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

SO_VER := 2
SO_MINOR := 0
SO_SUFFIX := $(SO_VER).$(SO_MINOR)
CFLAGS += -I$(CURDIR)/include

C_SRCS = bitmap.c kvutil.c pool.c
LIBNAME = kvstore_util

include $(SPDK_ROOT_DIR)/mk/spdk.lib.mk