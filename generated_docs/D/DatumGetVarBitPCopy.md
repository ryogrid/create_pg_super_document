# DatumGetVarBitPCopy

## Location
[src/include/utils/varbit.h:51-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/varbit.h#L51-L56)

## Overview
Converts a Datum value to a writable VarBit pointer, ensuring the result is a modifiable copy through TOAST decompression and copying.

## Definition
```c
static inline VarBit *DatumGetVarBitPCopy(Datum X)
```

## Detailed Description
DatumGetVarBitPCopy is an inline function that safely converts a Datum value containing a bit string (BIT or BIT VARYING type) to a VarBit pointer, ensuring that the result is always a modifiable copy. Unlike DatumGetVarBitP, this function guarantees that the returned pointer references a copy of the data that can be safely modified without affecting the original.

The function handles TOAST decompression and creates a copy when necessary, making it essential for functions that need to modify bit string data. This is part of PostgreSQL's fmgr interface macros for safe manipulation of toastable varlena types.

## Parameters / Member Variables
- `X`: A Datum value containing a bit string (BIT or BIT VARYING type) that may be TOASTed

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM_COPY
  - VarBit (type)
- Called from (representative examples):
  - PG_GETARG_VARBIT_P_COPY

## Notes and Other Information
- This is an inline function defined in src/include/utils/varbit.h
- Always returns a modifiable copy, making it safe for functions that need to modify bit string data
- More expensive than DatumGetVarBitP as it may need to allocate and copy memory
- Essential for write operations on bit string data to avoid modifying shared or read-only memory
- Part of PostgreSQL's type-safe Datum conversion system ensuring data integrity during modifications