SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

SO_VER := 2
SO_MINOR := 0
SO_SUFFIX := $(SO_VER).$(SO_MINOR)

CFLAGS += -I$(CURDIR)/src/include
CFLAGS += -I$(CURDIR)/src/indexes/include
CFLAGS += -I$(CURDIR)/src/utils/include

CFLAGS += -ffunction-sections
CFLAGS += -fdata-sections

C_SRCS  = $(shell ls src/worker/*.c)
C_SRCS += $(shell ls src/utils/*.c)
C_SRCS += $(shell ls src/slab/*.c)
C_SRCS += $(shell ls src/reclaim/*.c)
C_SRCS += $(shell ls src/pagechunk/*.c)
C_SRCS += $(shell ls src/kvs/*.c)
C_SRCS += $(shell ls src/iomgr/*.c)
C_SRCS += src/indexes/index.c src/indexes/impl/art.c src/indexes/impl/rbtree_uint.c

LIBNAME = pemon

include $(SPDK_ROOT_DIR)/mk/spdk.lib.mk