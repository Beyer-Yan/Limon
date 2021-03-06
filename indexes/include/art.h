#include <stdint.h>
#ifndef ART_H
#define ART_H

#ifdef __cplusplus
extern "C" {
#endif

#include "index.h"

#define NODE4   1
#define NODE16  2
#define NODE48  3
#define NODE256 4

#define MAX_PREFIX_LEN 10

#if defined(__GNUC__) && !defined(__clang__)
# if __STDC_VERSION__ >= 199901L && 402 == (__GNUC__ * 100 + __GNUC_MINOR__)
/*
 * GCC 4.2.2's C99 inline keyword support is pretty broken; avoid. Introduced in
 * GCC 4.2.something, fixed in 4.3.0. So checking for specific major.minor of
 * 4.2 is fine.
 */
#  define BROKEN_GCC_C99_INLINE
# endif
#endif

typedef int(*art_callback)(void *data, const unsigned char *key, uint32_t key_len, uint64_t value);

/**
 * This struct is included as part
 * of all the various node sizes
 */
typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint32_t partial_len;
    unsigned char partial[MAX_PREFIX_LEN];
} art_node;

/**
 * Small node with only 4 children
 */
typedef struct {
    art_node n;
    unsigned char keys[4];
    art_node *children[4];
} art_node4;

/**
 * Node with 16 children
 */
typedef struct {
    art_node n;
    unsigned char keys[16];
    art_node *children[16];
} art_node16;

/**
 * Node with 48 children, but
 * a full 256 byte field.
 */
typedef struct {
    art_node n;
    unsigned char keys[256];
    art_node *children[48];
} art_node48;

/**
 * Full node with 256 children
 */
typedef struct {
    art_node n;
    art_node *children[256];
} art_node256;

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
typedef struct {
    uint64_t value;
    uint32_t key_len;
    unsigned char key[];
} art_leaf;

/**
 * Main struct, points to root.
 */
typedef struct {
    art_node *root;
    uint64_t size;
} art_tree;

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int art_tree_init(art_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define init_art_tree(...) art_tree_init(__VA_ARGS__)

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int art_tree_destroy(art_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define destroy_art_tree(...) art_tree_destroy(__VA_ARGS__)

/**
 * Returns the size of the ART tree.
 */
#ifdef BROKEN_GCC_C99_INLINE
# define art_size(t) ((t)->size)
#else
inline uint64_t art_size(art_tree *t) {
    return t->size;
}
#endif

/**
 * Inserts a new value into the ART tree
 * @param t The tree
 * @param key The key
 * @param key_len The length of the key
 * @param value. If the key exists, the
 * old value will be replaced.
 */
uint64_t art_insert(art_tree *t, const unsigned char *key, int key_len, uint64_t value);

/**
 * Deletes a value from the ART tree. If the key exists, it will be deleted, otherwise nothing 
 * will be done.
 * @param t The tree
 * @param key The key
 * @param key_len The length of the key
 */
void art_delete(art_tree *t, const unsigned char *key, int key_len);

/**
 * Searches for a value in the ART tree
 * @param t The tree
 * @param key The key
 * @param key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
uint64_t art_search(const art_tree *t, const unsigned char *key, int key_len);

/**
 * Returns the minimum valued leaf
 * @return The minimum leaf or NULL
 */
art_leaf* art_minimum(art_tree *t);

/**
 * Returns the maximum valued leaf
 * @return The maximum leaf or NULL
 */
art_leaf* art_maximum(art_tree *t);

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @param t The tree to iterate over
 * @param cb The callback function to invoke
 * @param data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter(art_tree *t, art_callback cb, void *data);

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @param t The tree to iterate over
 * @param prefix The prefix of keys to read
 * @param prefix_len The length of the prefix
 * @param cb The callback function to invoke
 * @param data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int art_iter_prefix(art_tree *t, const unsigned char *prefix, int prefix_len, art_callback cb, void *data);

/**
 * @brief iterate all items, of which the key is greater than the given key. When the key is found,
 * the cb will be called. If the cb return 0, the iter will continue.   If the cb return non-0 value,
 * the iter will terminate.
 * 
 * @param t         The art tree pointer
 * @param key       The  base key. 
 * @param key_len   The length of base key
 * @param cb        User callback
 * @param data      User callback data
 * @return int      0 on sucess, or the return  of the callback.
 */
int art_iter_next(art_tree *t, const unsigned char *key, int key_len, art_callback cb, void*data);

#ifdef __cplusplus
}
#endif

#endif
