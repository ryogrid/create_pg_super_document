# DatumGetVarBitP

## Location
[src/include/utils/varbit.h:45-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/varbit.h#L45-L50)

## Overview
Converts a Datum value to a VarBit pointer, handling potential TOAST decompression for variable-length bit string data.

## Definition

```c
static inline VarBit *
DatumGetVarBitP(Datum X)
```
## Detailed Description
DatumGetVarBitP is an inline function that safely converts a Datum value containing a bit string (BIT or BIT VARYING type) to a VarBit pointer. The function automatically handles TOAST decompression if the bit string data has been compressed or stored out-of-line. This is essential for working with variable-length bit strings in PostgreSQL, as they are toastable varlena types that may need decompression before access.

The function serves as part of the fmgr (function manager) interface macros, providing a standardized way to extract bit string data from Datum values passed to PostgreSQL functions.

## Parameters / Member Variables
- `X`: A Datum value containing a bit string (BIT or BIT VARYING type) that may be TOASTed

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM
  - VarBit (type)
- Called from (representative examples):
  - PG_GETARG_VARBIT_P

## Notes and Other Information
- This is an inline function defined in src/include/utils/varbit.h
- BIT and BIT VARYING types share the same representation and use the same set of interface macros
- The function automatically handles TOAST decompression, making it safe to use with potentially compressed bit string data
- The returned VarBit pointer should not be modified directly as it may point to shared or read-only memory
- Part of PostgreSQL's type-safe Datum conversion system for bit string operations

## Simplified Source

```c
static inline VarBit *
DatumGetVarBitP(Datum X)
{
    // Convert Datum to VarBit pointer, handling TOAST decompression
    return (VarBit *) PG_DETOAST_DATUM(X);
}
```