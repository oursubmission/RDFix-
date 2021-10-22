#ifndef __KVS_H__
#define __KVS_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <string.h>

#define KVS_SPACE_INCREMENT 8

typedef int KVScompare(const char *a, const char *b);

typedef const void KVSkey;

typedef void KVSvalue;

typedef struct {
    KVSkey *key;
    KVSvalue *value;
} KVSpair;

typedef struct KVSstore KVSstore;

/** Create a new key-value store.

    @param compare
        A function to compare keys. If the store will only contain string keys,
        use strcmp, or use NULL for the default behavior of comparing memory
        addresses, or use a custom function matching the signature of strcmp.

    @return
        A pointer to the store.
*/
KVSstore *kvs_create(KVScompare *compare);

/** Destroy a key-value store.

    @param store
        A pointer to the store.
*/
void kvs_destroy(KVSstore *store);

/** Store a value.

    @param store
        A pointer to the store.

    @param key
        A key used to retrieve the value later. If the key already exists, the
        new value will be stored in place of the old one, unless value is NULL
        in which case the key-value pair will be removed from the store.

    @param value
        A pointer to the data being stored, or NULL to remove an existing value. 
*/
void kvs_put(KVSstore *store, KVSkey *key, KVSvalue *value);

/** Retrieve a value.

    @param store
        A pointer to the store.

    @param key
        A key used to retrieve the value.

    @return
        A pointer to the retrieved value, or NULL if not found.
*/
KVSvalue *kvs_get(KVSstore *store, KVSkey *key);

/** Remove a value from the store.

    @param store
        A pointer to the store.

    @param key
        A key identifying the value to be removed.
*/
void kvs_remove(KVSstore *store, KVSkey *key);

/** Get the number of values in a store.

    @param store
        A pointer to the store.

    @return
        The number of values contained in the store.
*/
size_t kvs_length(KVSstore *store);

/** Get a key-value pair at a given index.

    @param store
        A pointer to the store.

    @param index
        The index of the key-value pair.

    @return
        A pointer to the key-value pair, or NULL if the index is out or range.
*/
KVSpair *kvs_pair(KVSstore *store, size_t index);

#ifdef __cplusplus
}
#endif

#endif /* #define __KVS_H__ */

