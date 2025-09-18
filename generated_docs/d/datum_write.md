# datum_write

## Location
src/backend/utils/adt/rangetypes.c: 2709 - 2785

## Overview
A static function that writes a datum to a specified memory location with proper alignment and returns the updated pointer position.

## Definition


## Detailed Description
This function is part of PostgreSQL's range type serialization system and handles the physical writing of datum values to memory buffers. It supports multiple storage formats including pass-by-value types, variable-length arrays (varlena), C-strings, and fixed-length pass-by-reference types. The function optimizes storage by converting eligible varlena types to short format when possible and ensures proper memory alignment for each data type. It includes safety checks to prevent storing toast pointers within range objects.

## Parameters / Member Variables
- : Memory pointer where the datum should be written
- : The datum value to write
- : Whether the type is passed by value
- : Type alignment requirement ('c', 's', 'i', 'd')
- : Type length (-1 for varlena, -2 for cstring, positive for fixed length)
- : Type storage strategy ('p', 'e', 'm', 'x')

## Dependencies
- Functions called/Symbols referenced:
  - att_align_nominal
  - store_att_byval
  - VARATT_IS_EXTERNAL
  - VARATT_IS_SHORT
  - VARSIZE_SHORT
  - TYPE_IS_PACKABLE
  - VARATT_CAN_MAKE_SHORT
  - VARATT_CONVERTED_SHORT_SIZE
  - SET_VARSIZE_SHORT
  - VARDATA
  - VARSIZE
  - [DatumGetCString](../D/DatumGetCString.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - TYPALIGN_CHAR
- Called from (representative examples):
  - [range_serialize](../r/range_serialize.md)

## Notes and Other Information
This function implements comprehensive datum serialization logic with several optimization strategies. It converts eligible varlena types to short format to save space, handles alignment requirements correctly for different data types, and includes error checking to prevent toast pointer storage. The function is critical for range type persistence and is called for both lower and upper bound values during range serialization. It advances the pointer by the actual data length written, making it suitable for sequential writing operations.