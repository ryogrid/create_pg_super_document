# varatt_external

## Location
src/include/varatt.h: 32 - 39

## Overview
A structure representing a traditional "TOAST pointer" that contains the information needed to fetch a Datum stored out-of-line in a TOAST table.

## Definition


## Detailed Description
The varatt_external structure is a fundamental component of PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system. It serves as a pointer to data that has been stored out-of-line in a separate TOAST table due to its large size. The structure contains all the necessary information to locate and retrieve the original data from the TOAST table.

The data referenced by this pointer is compressed if and only if the external size stored in va_extinfo is less than va_rawsize - VARHDRSZ. This structure is designed to be stored unaligned within actual tuples and must not contain any padding to ensure consistency when using memcmp for equality comparisons.

## Parameters / Member Variables
- : The original size of the data including the header, representing the full size of the uncompressed data
- : Contains both the external saved size (without header) and compression method information
- : A unique identifier for the value within the TOAST table, used to locate the specific data
- : The relation ID of the TOAST table that contains the out-of-line data

## Dependencies
- Functions called/Symbols referenced:
  - int32 (PostgreSQL type)
  - uint32 (PostgreSQL type)
  - Oid (PostgreSQL type)
- Called from (representative examples):
  - detoast_attr_slice
  - toast_fetch_datum
  - toast_fetch_datum_slice
  - toast_save_datum
  - toast_delete_datum
  - ReorderBufferToastReplace

## Notes and Other Information
- This structure must not contain padding to ensure memcmp compatibility
- Data is stored unaligned within tuples, requiring memcpy to access fields safely
- Compression detection is performed by comparing va_extinfo with (va_rawsize - VARHDRSZ)
- Used extensively throughout PostgreSQL's TOAST system for managing large attribute values
- Critical for the out-of-line storage mechanism that allows PostgreSQL to handle very large data values efficiently