# gtsvector_compress

## Location
[src/backend/utils/adt/tsgistidx.c:172-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L172-L251)

## Overview
Compresses a TSVector entry for GiST indexing by converting it to either an array of hash values or a bit signature, depending on the size and nature of the input.

## Definition
```c
Datum gtsvector_compress(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of the GiST (Generalized Search Tree) support for TSVector data types. It performs compression on TSVector entries to optimize index storage and search performance. The function handles three main scenarios:

1. **Leaf keys (TSVector data)**: Converts TSVector words to an array of CRC32 hash values, sorts and deduplicates them. If the resulting array is too large (exceeds TOAST_INDEX_TARGET), it creates a bit signature instead.

2. **Existing signature keys**: Checks if all bits in the signature are set, and if so, marks it as ALLISTRUE for optimization.

3. **Already compressed entries**: Returns the entry unchanged if no further compression is needed.

The compression strategy balances between exact matching (hash arrays) for smaller datasets and approximate matching (bit signatures) for larger datasets to maintain index performance.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `entry`: GISTENTRY pointer containing the TSVector data to be compressed

## Dependencies
- Functions called/Symbols referenced:
  - [gtsvector_alloc](gtsvector_alloc.md): Allocates memory for SignTSVector structures
  - [DatumGetTSVector](../D/DatumGetTSVector.md): Extracts TSVector from Datum
  - `INIT_LEGACY_CRC32`, `COMP_LEGACY_CRC32`, `FIN_LEGACY_CRC32`: CRC32 hash computation
  - `qsort`: Sorts hash array
  - [qunique](../q/qunique.md): Removes duplicate hash values
  - [makesign](../m/makesign.md): Creates bit signature from hash array
  - `gistentryinit`: Initializes GiST entry structure
  - [compareint](../c/compareint.md): Integer comparison function for sorting
- Called from:
  - GiST index operations (no direct references found in current analysis)

## Notes and Other Information
- Uses CRC32 hashing to convert TSVector words into integers for efficient comparison
- Implements a two-tier compression strategy: hash arrays for smaller data, bit signatures for larger data
- The TOAST_INDEX_TARGET threshold determines when to switch from hash arrays to bit signatures
- Handles hash collisions by removing duplicates after sorting
- Optimizes signature storage by detecting when all signature bits are set (ALLISTRUE condition)
- Part of the PostgreSQL full-text search indexing infrastructure

## Simplified Source

```c
Datum gtsvector_compress(PG_FUNCTION_ARGS) {
    GISTENTRY *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
    int siglen = GET_SIGLEN();
    GISTENTRY *retval = entry;

    if (entry->leafkey) {
        // Process leaf keys (TSVector data)
        TSVector val = DatumGetTSVector(entry->key);
        SignTSVector *res = gtsvector_alloc(ARRKEY, val->size, NULL);
        int32 *arr = GETARR(res);
        WordEntry *ptr = ARRPTR(val);
        char *words = STRPTR(val);

        // Convert each word to CRC32 hash
        for (int32 len = val->size; len > 0; len--, arr++, ptr++) {
            pg_crc32 c;
            INIT_LEGACY_CRC32(c);
            COMP_LEGACY_CRC32(c, words + ptr->pos, ptr->len);
            FIN_LEGACY_CRC32(c);
            *arr = *(int32 *) &c;
        }

        // Sort and remove duplicates
        qsort(GETARR(res), val->size, sizeof(int), compareint);
        int len = qunique(GETARR(res), val->size, sizeof(int), compareint);

        // Resize if duplicates were removed
        if (len != val->size) {
            int newsize = CALCGTSIZE(ARRKEY, len);
            res = (SignTSVector *) repalloc(res, newsize);
            SET_VARSIZE(res, newsize);
        }

        // Create bit signature if array too large
        if (VARSIZE(res) > TOAST_INDEX_TARGET) {
            SignTSVector *ressign = gtsvector_alloc(SIGNKEY, siglen, NULL);
            makesign(GETSIGN(ressign), res, siglen);
            res = ressign;
        }

        // Create new GIST entry
        retval = (GISTENTRY *) palloc(sizeof(GISTENTRY));
        gistentryinit(*retval, PointerGetDatum(res),
                      entry->rel, entry->page, entry->offset, false);
    }
    else if (ISSIGNKEY(DatumGetPointer(entry->key)) &&
             !ISALLTRUE(DatumGetPointer(entry->key))) {
        // Check if signature has all bits set
        BITVECP sign = GETSIGN(DatumGetPointer(entry->key));

        // Check each byte for non-0xff values
        for (int32 i = 0; i < siglen; i++) {
            if ((sign[i] & 0xff) != 0xff)
                PG_RETURN_POINTER(retval);  // Not all bits set
        }

        // All bits set - mark as ALLISTRUE
        SignTSVector *res = gtsvector_alloc(SIGNKEY | ALLISTRUE, siglen, sign);
        retval = (GISTENTRY *) palloc(sizeof(GISTENTRY));
        gistentryinit(*retval, PointerGetDatum(res),
                      entry->rel, entry->page, entry->offset, false);
    }

    PG_RETURN_POINTER(retval);
}
```