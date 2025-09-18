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