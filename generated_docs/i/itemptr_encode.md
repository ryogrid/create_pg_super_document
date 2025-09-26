# itemptr_encode

## Location
[src/include/catalog/index.h:189-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/index.h#L189-L209)

## Overview
Encodes an ItemPointer (TID) as a 64-bit integer value that preserves the sorting order of the original TID values for performance optimization during index validation.

## Definition

```c
static inline int64
itemptr_encode(ItemPointer itemptr)
```
## Detailed Description
The  function converts an ItemPointer (tuple identifier) into a 64-bit integer representation while maintaining the same sorting characteristics as the original TID values. This encoding is used primarily during index validation operations where faster comparison of TID values is beneficial.

The encoding scheme uses:
- 16 least significant bits for the offset number
- 32 adjacent bits for the block number  
- Remaining bits are unused, ensuring no negative values in two's complement representation

The function extracts the block number and offset from the ItemPointer, then combines them using bitwise operations to create a sortable 64-bit integer.

## Parameters / Member Variables
- : The ItemPointer (TID) to be encoded into a 64-bit integer

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
- Called from (representative examples):
  - [validate_index_callback](../v/validate_index_callback.md)

## Notes and Other Information
- This encoding produces int64 values that sort in the same order as their corresponding TID values using the default int8 opclass
- The encoding is designed to be significantly faster than direct TID comparisons during index validation
- The function assumes a two's complement representation for integers
- Block numbers occupy bits 16-47, while offset numbers occupy bits 0-15 of the resulting 64-bit value