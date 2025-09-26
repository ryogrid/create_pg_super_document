# itemptr_decode

## Location
[src/include/catalog/index.h:210-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/index.h#L210-L218)

## Overview
Decodes a 64-bit integer representation back into an ItemPointer (TID) structure, reversing the encoding performed by itemptr_encode.

## Definition
static inline void itemptr_decode(ItemPointer itemptr, int64 encoded)

## Detailed Description
The itemptr_decode function converts a 64-bit integer encoded representation back to its original ItemPointer (tuple identifier) form. This function is the inverse operation of itemptr_encode, extracting the block number and offset from the encoded integer using bitwise operations.

The decoding process:
- Extracts the block number from bits 16-47 by right-shifting 16 positions
- Extracts the offset number from bits 0-15 using a bitwise AND with 0xFFFF mask
- Reconstructs the ItemPointer using ItemPointerSet

## Parameters / Member Variables
- itemptr: Output parameter - the ItemPointer structure to be populated with decoded values
- encoded: The 64-bit integer representation to be decoded back to ItemPointer

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerSet
- Called from (representative examples):
  - heapam_index_validate_scan

## Notes and Other Information
- This function is the inverse of itemptr_encode and must be used with values encoded by that function
- The decoding assumes the same bit layout as used in itemptr_encode (16 bits for offset, 32 bits for block number)
- Used primarily during index validation operations where encoded TID values need to be converted back to their original form
- The function modifies the ItemPointer structure passed as the first parameter