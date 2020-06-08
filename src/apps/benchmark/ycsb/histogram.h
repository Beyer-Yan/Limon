#ifndef HISTOGRAM_H
#define HISTOGRAM_H

#include <stdint.h>

void histogram_init(void);
void histogram_reset(void);
void histogram_update(uint64_t data);
void histogram_print(void);

#endif
