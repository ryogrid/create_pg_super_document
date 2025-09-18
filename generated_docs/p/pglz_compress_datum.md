# pglz_compress_datum

## Location
src/backend/access/common/toast_compression.c: 40 - 81

## Overview
Compresses a varlena data structure using the PGLZ compression algorithm, which is PostgreSQL's default compression method for TOAST (The Oversized-Attribute Storage Technique).

## Definition


## Detailed Description
This function implements PGLZ compression for PostgreSQL's TOAST system. It takes a varlena structure containing data and attempts to compress it using the PGLZ algorithm. The function performs size validation to ensure the input data falls within acceptable compression boundaries before attempting compression. If compression is successful and results in space savings, it returns a new compressed varlena structure; otherwise, it returns NULL to indicate compression failure or that compression would not be beneficial.

The function allocates memory for the maximum possible compressed output size plus varlena overhead, performs the compression operation, and properly sets the compressed size header if successful.

## Parameters / Member Variables
- : A pointer to the input varlena structure containing the data to be compressed

## Dependencies
- Functions called/Symbols referenced:
  - VARSIZE_ANY_EXHDR (macro to get data size excluding header)
  - PGLZ_strategy_default (compression strategy settings)
  - [palloc](palloc.md) (PostgreSQL memory allocation)
  - PGLZ_MAX_OUTPUT (macro to calculate max compressed size)
  - VARHDRSZ_COMPRESSED (compressed varlena header size)
  - pglz_compress (core PGLZ compression function)
  - VARDATA_ANY (macro to get data portion of varlena)
  - [pfree](pfree.md) (PostgreSQL memory deallocation)
  - SET_VARSIZE_COMPRESSED (macro to set compressed size header)
- Called from (representative examples):
  - [toast_compress_datum](../t/toast_compress_datum.md) (in src/backend/access/common/toast_internals.c:67)
  - Referenced in CompressionMethodIsValid (in src/include/access/toast_compression.h:57)

## Notes and Other Information
- Returns NULL if the input size is outside the acceptable range for PGLZ compression (below min_input_size or above max_input_size)
- Returns NULL if compression fails or doesn't achieve sufficient space savings
- The function handles all memory management internally, freeing allocated memory on compression failure
- Part of PostgreSQL's TOAST compression infrastructure, specifically handling PGLZ method
- Located in src/backend/access/common/toast_compression.c:40-81