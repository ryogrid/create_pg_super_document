# DatumGetMacaddrP

## Location
src/include/utils/inet.h: 147 - 152

## Overview
Converts a Datum value to a pointer to a macaddr structure for MAC address operations.

## Definition
```c
static inline macaddr *
DatumGetMacaddrP(Datum X)
```

## Detailed Description
DatumGetMacaddrP is an inline function that extracts a macaddr pointer from a Datum value. Unlike the inet functions, this function uses DatumGetPointer directly because macaddr is a fixed-length pass-by-reference datatype that does not require detoasting. MAC addresses have a fixed 6-byte length, so they are stored directly without compression or variable-length encoding.

This function is part of PostgreSQL's fmgr (function manager) interface macros for the macaddr data type, providing efficient access to MAC address data.

## Parameters / Member Variables
- `X`: A Datum value containing a macaddr structure

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](DatumGetPointer.md)
  - macaddr
- Called from (representative examples):
  - [macaddr_fast_cmp](../m/macaddr_fast_cmp.md)
  - [macaddr_abbrev_convert](../m/macaddr_abbrev_convert.md)
  - [convert_network_to_scalar](../c/convert_network_to_scalar.md)
  - PG_GETARG_MACADDR_P (macro)

## Notes and Other Information
- This is an inline function defined in src/include/utils/inet.h for performance
- macaddr is a fixed-length datatype, so no detoasting is required
- Used for MAC address comparison operations and data conversion
- Part of the PostgreSQL function manager interface for type conversion
- Much simpler than inet conversion functions due to fixed-length nature of MAC addresses
- The PG_GETARG_MACADDR_P macro provides a convenient wrapper for function argument access