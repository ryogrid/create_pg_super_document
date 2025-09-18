# _hash_firstfreebit

## Location
src/backend/access/hash/hashovfl.c: 448 - 489

## Overview
Finds and returns the bit position of the first unset (free) bit in a 32-bit bitmap word, used for locating available overflow pages within bitmap segments.

## Definition
```c
static uint32 _hash_firstfreebit(uint32 map)
```

## Detailed Description
This utility function performs a linear scan through the bits of a 32-bit word to find the first bit that is not set (value 0). The function is essential for overflow page allocation within hash index bitmap pages, where each bit represents the allocation status of an overflow page.

The function iterates through each bit position from 0 to BITS_PER_MAP-1, using a shifting mask to test individual bits. When it finds the first unset bit, it returns that bit's position. If all bits are set (indicating all overflow pages in this segment are allocated), the function raises an error since it should only be called when free space is expected to exist.

## Parameters / Member Variables
- `map`: A 32-bit word representing a segment of the overflow page bitmap, where each bit indicates whether an overflow page is allocated (1) or free (0)

## Dependencies
- Functions called/Symbols referenced:
  - BITS_PER_MAP (constant defining the number of bits per bitmap word)
  - elog/ERROR (error reporting when no free bit is found)
- Called from (representative examples):
  - [_hash_addovflpage](_hash_addovflpage.md) (when searching for free overflow pages in bitmap)

## Notes and Other Information
- This is a static function, only accessible within the hashovfl.c module
- The function assumes that the caller has already verified that the map word contains at least one free bit
- Returns bit positions in the range 0 to BITS_PER_MAP-1 (typically 0-31 for 32-bit words)
- The error condition should never occur in normal operation, as the function is only called when freep[j] != ALL_SET
- Uses a simple but efficient linear scan approach rather than more complex bit manipulation techniques
- The shifting mask approach ensures portability across different architectures