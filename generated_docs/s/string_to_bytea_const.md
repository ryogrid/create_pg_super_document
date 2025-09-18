# string_to_bytea_const

## Location
src/backend/utils/adt/like_support.c: 1787 - 1797

## Overview
A static utility function that creates a PostgreSQL Const node of bytea type from a binary C string and its length, used in LIKE pattern matching operations.

## Definition


## Detailed Description
This function converts a binary C string into a PostgreSQL Const node containing a bytea value. It allocates memory for a bytea structure, copies the input string data, sets the appropriate variable-length header size, and wraps it in a Const node. This is primarily used in LIKE pattern matching support to create bytea constants for pattern prefix operations.

The function performs the following steps:
1. Allocates memory for a bytea structure (header + data length)
2. Copies the input string data to the bytea's data area
3. Sets the variable-length header size using PostgreSQL's VARSIZE mechanism
4. Creates a Datum from the bytea pointer
5. Wraps the Datum in a Const node with appropriate type information

## Parameters / Member Variables
- : The input binary C string to be converted to bytea
- : The length of the input string in bytes

## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation)
  -  (macro to get variable-length data area)
  -  (macro to set variable-length size)
  -  (convert pointer to Datum)
  -  (create Const node)
- Called from:
  -  (src/backend/utils/adt/like_support.c:108)
  -  (src/backend/utils/adt/like_support.c:1079)
  -  (src/backend/utils/adt/like_support.c:1681)

## Notes and Other Information
- This is a static function, only accessible within the like_support.c file
- The function creates a Const node with BYTEAOID type identifier
- Uses InvalidOid for collation and typmod parameters in makeConst
- The returned Const node has isnull=false and isbyval=false flags
- Memory allocated via palloc will be automatically freed by PostgreSQL's memory context system
- Located in src/backend/utils/adt/like_support.c at lines 1787-1797