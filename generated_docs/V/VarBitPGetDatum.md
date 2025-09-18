# VarBitPGetDatum

## Location
[src/include/utils/varbit.h:57-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/varbit.h#L57-L61)

## Overview
Converts a VarBit pointer to a Datum value for return from PostgreSQL functions or storage in the system.

## Definition
```c
static inline Datum VarBitPGetDatum(const VarBit *X)
```

## Detailed Description
VarBitPGetDatum is an inline function that converts a VarBit pointer to a Datum value. This function serves as the complement to DatumGetVarBitP and DatumGetVarBitPCopy, completing the type conversion interface for bit string data in PostgreSQL's function manager system.

The function simply wraps the pointer in a Datum type, allowing bit string results to be returned from PostgreSQL functions or stored in the system's internal data structures. This is part of the standard pattern for handling varlena types in PostgreSQL.

## Parameters / Member Variables
- `X`: A const pointer to a VarBit structure representing bit string data

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - VarBit (type)
- Called from (representative examples):
  - [bitshiftleft](../b/bitshiftleft.md)
  - [bitshiftright](../b/bitshiftright.md)
  - PG_RETURN_VARBIT_P

## Notes and Other Information
- This is an inline function defined in src/include/utils/varbit.h
- Takes a const pointer, indicating it does not modify the VarBit data
- Used primarily for returning bit string results from PostgreSQL functions
- Part of the standard fmgr interface pattern for varlena types
- The const qualifier on the parameter emphasizes that this is for output/return purposes only
- Essential for completing the round-trip conversion between VarBit pointers and Datum values