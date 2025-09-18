# MacaddrPGetDatum

## Location
src/include/utils/inet.h: 153 - 157

## Overview
Converts a macaddr pointer to a Datum value for use in PostgreSQL's function manager interface.

## Definition
```c
static inline Datum
MacaddrPGetDatum(const macaddr *X)
```

## Detailed Description
MacaddrPGetDatum is an inline function that converts a pointer to a macaddr structure into a Datum value. It simply wraps the PointerGetDatum function, providing a type-safe interface specifically for macaddr data types. Since macaddr is a fixed-length pass-by-reference datatype (6 bytes for MAC addresses), no special handling for packing or compression is needed.

This function is part of PostgreSQL's fmgr (function manager) interface macros for the macaddr data type, enabling macaddr pointers to be passed as return values or arguments in the PostgreSQL function call interface.

## Parameters / Member Variables
- `X`: A constant pointer to a macaddr structure to be converted to Datum

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - macaddr
- Called from (representative examples):
  - PG_RETURN_MACADDR_P (macro)

## Notes and Other Information
- This is an inline function defined in src/include/utils/inet.h for performance
- Takes a const pointer parameter, indicating the macaddr data should not be modified through this interface
- Much simpler than inet conversion functions due to fixed-length nature of MAC addresses
- Part of the PostgreSQL function manager interface for type conversion
- The PG_RETURN_MACADDR_P macro provides a convenient wrapper for function return statements
- No detoasting or special memory management required due to fixed-length datatype