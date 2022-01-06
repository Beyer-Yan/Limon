SPDK_ROOT_DIR := $(abspath $(CURDIR)/../spdk)
#SPDK_ROOT_DIR := /home/buaa/yby/spdk
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

SO_VER := 2
SO_MINOR := 0
SO_SUFFIX := $(SO_VER).$(SO_MINOR)

CFLAGS += -I$(CURDIR)/include
CFLAGS += -I$(CURDIR)/indexes/include
CFLAGS += -I$(CURDIR)/utils/include

CFLAGS += -g

#CFLAGS += -ffunction-sections
#CFLAGS += -fdata-sections

C_SRCS  = $(shell ls worker/*.c)
C_SRCS += $(shell ls utils/*.c)
C_SRCS += $(shell ls slab/*.c)
C_SRCS += $(shell ls pagechunk/*.c)
C_SRCS += $(shell ls kvs/*.c)
C_SRCS += io/io_load.c io/io_store_batch.c
#C_SRCS += src/io/io_load.c src/io/io_store.c
#C_SRCS += src/indexes/index.c src/indexes/impl/art.c src/indexes/impl/rbtree_uint.c src/indexes/impl/hashmap.c

C_SRCS += indexes/index.c indexes/impl/rbtree_uint.c

LIBNAME = limon

SPDK_MAP_FILE = $(SPDK_ROOT_DIR)/mk/spdk_blank.map
include $(SPDK_ROOT_DIR)/mk/spdk.lib.mk
