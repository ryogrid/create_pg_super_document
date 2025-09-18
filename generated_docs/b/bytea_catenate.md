# bytea_catenate

## Location
[src/backend/utils/adt/varlena.c:2953-2985](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2953-L2985)

## Overview
bytea_catenate is a static internal function that performs the core concatenation logic for bytea values, serving as the implementation foundation for byteacat and other bytea manipulation functions.

## Definition


## Detailed Description
This function performs the actual concatenation of two bytea values by allocating new memory and copying data from both input bytea values sequentially. It handles the PostgreSQL variable-length data structure details, including proper header management and memory allocation. The function includes safety checks for negative lengths and supports arguments in short-header form, though not compressed or out-of-line variants.

## Parameters / Member Variables
- : First bytea value to concatenate
- : Second bytea value to concatenate
- Returns: Newly allocated bytea containing the concatenated result

## Dependencies
- Functions called/Symbols referenced:
  - VARSIZE_ANY_EXHDR (to get data length excluding header)
  - [palloc](../p/palloc.md) (for memory allocation)
  - SET_VARSIZE (to set the variable-length header size)
  - VARDATA (to get data pointer of result)
  - VARDATA_ANY (to get data pointer from input bytea)
  - memcpy (to copy data)
- Called from (representative examples):
  - [byteacat](byteacat.md) (primary wrapper function)
  - [bytea_overlay](bytea_overlay.md) (for string overlay operations)

## Notes and Other Information
- This is a static function, only accessible within varlena.c
- Includes paranoia checks for negative lengths, setting them to 0 rather than throwing errors
- Supports short-header form arguments but explicitly does not handle compressed or out-of-line data
- Uses standard PostgreSQL memory management (palloc) for result allocation
- The function is designed to be reusable by other bytea manipulation functions
- Located in src/backend/utils/adt/varlena.c:2953-2985