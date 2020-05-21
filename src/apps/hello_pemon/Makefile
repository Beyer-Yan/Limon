SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../../../..)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk
include $(SPDK_ROOT_DIR)/mk/spdk.modules.mk

KVS_ROOT_DIR := $(abspath $(CURDIR)/../..)
CFLAGS += -I$(KVS_ROOT_DIR)/include
CFLAGS += -I$(KVS_ROOT_DIR)/indexes/include
CFLAGS += -I$(KVS_ROOT_DIR)/utils/include
#LDFLAGS += -Wl,--gc-sections

APP = hello

C_SRCS := hello.c

SPDK_LIB_LIST = $(ALL_MODULES_LIST)
SPDK_LIB_LIST += event_bdev event_accel event_vmd
SPDK_LIB_LIST += bdev accel event thread util conf trace \
		log jsonrpc json rpc sock notify
SPDK_LIB_LIST += pemon

include $(SPDK_ROOT_DIR)/mk/spdk.app.mk
