# bpcharoctetlen

## Location
src/backend/utils/adt/varchar.c: 709 - 726

## Overview
Returns the octet length (byte length) of a BPCHAR (blank-padded character) value without detoasting the input.

## Definition


## Detailed Description
This function calculates and returns the octet (byte) length of a BPCHAR data type. It uses an optimized approach by calling  to determine the size without actually detoasting the potentially compressed or out-of-line stored value. The function subtracts  (variable header size) from the raw datum size to get the actual data length, as BPCHAR values include a variable-length header that stores metadata about the value.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments and context

## Dependencies
- Functions called/Symbols referenced:
  - [toast_raw_datum_size](../t/toast_raw_datum_size.md)
  - PG_GETARG_DATUM (macro)
  - PG_RETURN_INT32 (macro)
  - VARHDRSZ (constant)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is optimized for performance by avoiding unnecessary detoasting operations
- The function works with the TOAST (The Oversized-Attribute Storage Technique) system to handle large or compressed values efficiently
- Returns the byte length as an int32 value
- Part of the BPCHAR (blank-padded character) data type implementation in PostgreSQL