#SPDK_ROOT_DIR := $(abspath $(CURDIR)/../..)
SPDK_ROOT_DIR := /home/buaa/yby/spdk

include $(SPDK_ROOT_DIR)/mk/spdk.common.mk
include $(SPDK_ROOT_DIR)/mk/spdk.modules.mk

KVS_ROOT_DIR := $(abspath $(CURDIR)/../..)
CFLAGS += -I$(KVS_ROOT_DIR)/include
CFLAGS += -I$(KVS_ROOT_DIR)/indexes/include
CFLAGS += -I$(KVS_ROOT_DIR)/utils/include

LDFLAGS += -L$(KVS_ROOT_DIR)/lib
LDFLAGS += -Wl,-rpath=$(KVS_ROOT_DIR)/lib

SYS_LIBS += -lm
SYS_LIBS += -ljemalloc
SYS_LIBS += -lcart

APP = kvs_bench
C_SRCS := kvs_bench.c
C_SRCS += $(shell ls ycsb/*.c)

SPDK_LIB_LIST = $(ALL_MODULES_LIST)
SPDK_LIB_LIST += event_bdev event_accel event_vmd
SPDK_LIB_LIST += bdev accel event thread util conf trace log jsonrpc json rpc sock notify
SPDK_LIB_LIST += limon

include $(SPDK_ROOT_DIR)/mk/spdk.app.mk
