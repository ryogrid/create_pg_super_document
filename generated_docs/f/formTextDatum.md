# formTextDatum

## Location
src/test/modules/spgist_name_ops/spgist_name_ops.c: 52 - 76

## Overview
A utility function that constructs a PostgreSQL text datum from a character string, optimizing storage by using short varlena header format when possible.

## Definition


## Detailed Description
This function creates a properly formatted PostgreSQL text datum from raw character data. It implements an optimization by choosing between short and standard varlena header formats based on the data length, which can save storage space for smaller text values. The function handles memory allocation, header setup, and data copying to create a valid PostgreSQL text value that can be stored in the database or used in index operations.

The function uses PostgreSQL's variable-length data (varlena) format, which includes a header indicating the total size followed by the actual data. For smaller values, it uses a compressed header format to reduce overhead.

## Parameters / Member Variables
- `data`: Pointer to the source character string (not necessarily null-terminated)
- `datalen`: Length of the source data in bytes

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_SHORT_MAX (maximum size for short varlena format)
  - VARHDRSZ_SHORT (size of short varlena header)
  - SET_VARSIZE_SHORT (macro to set short varlena size)
  - SET_VARSIZE (macro to set standard varlena size)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - memcpy (memory copy function)
  - [PointerGetDatum](../P/PointerGetDatum.md) (converts pointer to Datum)
- Called from (representative examples):
  - [spg_text_choose](../s/spg_text_choose.md)
  - [spg_text_picksplit](../s/spg_text_picksplit.md)  
  - [spgist_name_choose](../s/spgist_name_choose.md)
  - [spgist_name_compress](../s/spgist_name_compress.md)

## Notes and Other Information
- Located in src/backend/access/spgist/spgtextproc.c:113-137
- Static function, only accessible within the same compilation unit
- Optimizes storage by using short headers when data length + short header size ≤ VARATT_SHORT_MAX
- Essential for SP-GiST text processing operations, particularly in choose and picksplit functions
- Handles both empty and non-empty strings correctly
- The created datum must eventually be freed by PostgreSQL's memory management system