#include "histogram.h"
#include "spdk/env.h"
#include "spdk/histogram_data.h"

static uint64_t g_tsc_rate;
static struct spdk_histogram_data *g_his_data;
static double g_min,g_max,g_avg;

static const double g_latency_cutoffs[] = {
	0.01,
	0.10,
	0.25,
	0.50,
	0.75,
	0.90,
	0.95,
	0.98,
	0.99,
	0.995,
	0.999,
	0.9999,
	0.99999,
	0.999999,
	0.9999999,
	-1,
};

static void
_check_cutoff(void *ctx, uint64_t start, uint64_t end, uint64_t count,
	     uint64_t total, uint64_t so_far)
{
	double so_far_pct;
	double **cutoff = ctx;

	if (count == 0) {
		return;
	}

    double cur = (double)end * 1000 * 1000 / g_tsc_rate;

    g_min = cur < g_min ? cur : g_min;
    g_max = cur > g_max ? cur : g_max;
    g_avg += cur/total;

	so_far_pct = (double)so_far / total;
	while (so_far_pct >= **cutoff && **cutoff > 0) {
		printf("%9.5f%% : %9.3fus\n", **cutoff * 100, cur);
		(*cutoff)++;
	}
}

void histogram_init(void){
    g_his_data = spdk_histogram_data_alloc();
    g_tsc_rate = spdk_get_ticks_hz();
}

void histogram_reset(void){
    assert(g_his_data!=NULL);
    g_min = 0;
    g_max = 0;
    g_avg = 0;
    spdk_histogram_data_reset(g_his_data);
}

void histogram_update(uint64_t data){
    assert(g_his_data!=NULL);
    spdk_histogram_data_tally(g_his_data,data);
}

void histogram_print(void){
    assert(g_his_data!=NULL);
    printf("Summary latency histogram\n");
    printf("=================================================================================\n");

    const double *cutoff = g_latency_cutoffs;
    spdk_histogram_data_iterate(g_his_data, _check_cutoff, &cutoff);
    printf("min:%9.3fus\n",g_min);
    printf("avg:%9.3fus\n",g_avg);
    printf("max:%9.3fus\n",g_max);
    printf("\n");
}

