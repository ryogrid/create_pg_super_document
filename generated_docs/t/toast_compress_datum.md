# toast_compress_datum

## Location
[src/backend/access/common/toast_internals.c:46-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_internals.c#L46-L118)

## Overview
Creates a compressed version of a varlena datum using either PGLZ or LZ4 compression algorithms, returning NULL if compression would not provide sufficient space savings.

## Definition

```c
struct varlena *tmp = NULL;
```
## Detailed Description
This function attempts to compress a varlena datum using the specified compression method. It performs several validation checks to ensure the datum is compressible and verifies that compression actually provides meaningful space savings. The function supports two compression methods: PGLZ (PostgreSQL's traditional compression) and LZ4 (faster alternative). If the compression method parameter is invalid, it defaults to the system's configured default compression method.

The function implements a cost-benefit analysis by requiring that compressed data be at least 3 bytes smaller than the original to account for header overhead and potential alignment padding. This prevents scenarios where minimal compression gains could result in net space loss due to structural overhead.

## Parameters / Member Variables
- : The input Datum containing the varlena data structure to be compressed
- : Character indicating the compression method to use (TOAST_PGLZ_COMPRESSION or TOAST_LZ4_COMPRESSION)

## Dependencies
- Functions called/Symbols referenced:
  - [pglz_compress_datum](../p/pglz_compress_datum.md)
  - [lz4_compress_datum](../l/lz4_compress_datum.md)
  - CompressionMethodIsValid
  - VARATT_IS_EXTERNAL
  - VARATT_IS_COMPRESSED
  - VARSIZE_ANY_EXHDR
  - VARSIZE
  - TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [brin_form_tuple](../b/brin_form_tuple.md)
  - [index_form_tuple_context](../i/index_form_tuple_context.md)
  - [toast_tuple_try_compression](toast_tuple_try_compression.md)

## Notes and Other Information
- The function validates that input data is not already external or compressed
- Compression is only considered successful if it saves more than 2 bytes to account for header and alignment overhead
- Uses VAR{SIZE,DATA}_ANY macros to handle short varlenas efficiently without unnecessary copying
- Returns NULL for incompressible data, requiring callers to handle this case appropriately
- The compression method selection supports fallback to system default when an invalid method is specified