/* Copyright (c) 2011 the authors listed at the following URL, and/or
the authors of referenced articles or incorporated external code:
http://en.literateprograms.org/Red-black_tree_(C)?action=history&offset=20090121005050

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Retrieved from: http://en.literateprograms.org/Red-black_tree_(C)?oldid=16016
... and modified for even more speed and awesomeness...
*/

#ifndef _RBTREE_H_
#define _RBTREE_H_ 1
#include <unistd.h>
#include <stdint.h>
enum rbtree_node_color { RED, BLACK };

typedef struct rbtree_node_t {
    uint32_t key;
    void* value;
    struct rbtree_node_t* left;
    struct rbtree_node_t* right;
    struct rbtree_node_t* parent;
    enum rbtree_node_color color;
} *rbtree_node;

typedef struct rbtree_t {
    rbtree_node root;
    rbtree_node last_visited_node;
    int nb_elements;
} *rbtree;

typedef int (*compare_func)(uint32_t left, uint32_t right);
int uint_cmp(uint32_t left, uint32_t right){
    return left<right ? -1 : ((left==right)?0:1);
}

inline void* rbtree_lookup(rbtree t, uint32_t key){
    return rbtree_lookup_impl(t, key, uint_cmp);
}

inline void rbtree_insert(rbtree t, uint32_t key, void* value, compare_func compare){
    rbtree_insert_impl(t, key, value, uint_cmp);
}

inline void rbtree_delete(rbtree t, uint32_t key, compare_func compare){
    rbtree_delete_impl(t, key, uint_cmp);
}

inline void* rbtree_first(rbtree t){
    rbtree_node	node;
	if (!t){
		return NULL; 
    }  
    node = t->root;
    if(!node){
        return NULL;
    }

	while (node->left)
		node = node->left;
	return node->value;
}

inline void* rbtree_last(rbtree t){
    rbtree_node	node;
	if (!t){
		return NULL; 
    }  
    node = t->root;
    if(!node){
        return NULL;
    }
	while (node->right)
		node = node->right;
	return node->value;
}

rbtree rbtree_create();
void* rbtree_lookup_impl(rbtree t, uint32_t key, compare_func compare);
void rbtree_insert_impl(rbtree t, uint32_t key, void* value, compare_func compare);
void rbtree_delete_impl(rbtree t, uint32_t key, compare_func compare);

#endif
