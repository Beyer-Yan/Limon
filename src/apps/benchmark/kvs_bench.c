#include <stdint.h>
#include <stdbool.h>
#include "kvs.h"
#include "slab.h"
#include "kvutil.h"

#include "spdk/stdinc.h"

#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/blob_bdev.h"
#include "spdk/blob.h"
#include "spdk/log.h"
#include "spdk/string.h"

static int
_kvs_parse_arg(int ch, char *arg){
}

static void
_kvs_usage(){

}

int
main(int argc, char **argv){
    struct spdk_app_opts opts = {};
	int rc = 0;
	struct kvs_format_ctx *kctx = NULL;

	spdk_app_opts_init(&opts);

	opts.name = "kvs_run";
	if ((rc = spdk_app_parse_args(argc, argv, &opts, _g_kvs_getopt_string, _g_app_long_cmdline_options,
				      _kvs_parse_arg, _kvs_usage)) !=
	    SPDK_APP_PARSE_ARGS_SUCCESS) {
		exit(rc);
	}
	return rc;
}
