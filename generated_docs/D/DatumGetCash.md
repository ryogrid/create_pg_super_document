# DatumGetCash

## Location
src/include/utils/cash.h: 21 - 26

## Overview
DatumGetCash is an inline utility function that converts a Datum value to a Cash value, leveraging the underlying int64 representation of the Cash type.

## Definition


## Detailed Description
DatumGetCash is a simple conversion function that extracts a Cash value from a Datum. Since Cash is typedef'd as int64, this function directly delegates to DatumGetInt64() to perform the conversion. The function is marked as static inline for performance optimization, as it's a simple wrapper that should be inlined at compile time. This function is part of PostgreSQL's type system infrastructure, providing a standardized way to extract Cash values from the generic Datum container type.

## Parameters / Member Variables
- : The Datum value to be converted to Cash type

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt64](DatumGetInt64.md)
- Called from (representative examples):
  - PG_GETARG_CASH

## Notes and Other Information
- Cash is typedef'd as int64, making it an 8-byte signed integer
- The function leverages the fact that Cash and int64 have identical memory representation
- Pass-by-reference behavior matches that of int64 type
- This is a header-only inline function defined in src/include/utils/cash.h
- Part of PostgreSQL's monetary data type implementation