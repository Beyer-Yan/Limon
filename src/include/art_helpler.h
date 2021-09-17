#include <stdint.h>

/**
 * @brief art tree wrap for low-level cpp version
 * 
 */
struct art;

struct art* art_new(void);

void art_destroy(struct art*);

int art_put(struct art* t, uint64_t key, uint64_t val);

uint64_t art_get(struct art* t, uint64_t key);

int art_del(struct art* t, uint64_t key);

int art_scan(struct art* t, uint64_t start_key, int maxLen, int* founds, uint64_t *val_array);
