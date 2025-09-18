# BinarySearchRange

## Location
src/backend/utils/mb/conversion_procs/euc_tw_and_big5/big5.c: 207 - 291

## Overview
BinarySearchRange is a static function that performs binary search on a sorted array of character code mappings to find the appropriate conversion between Big5 and CNS 11643-1992 character encodings.

## Definition


## Detailed Description
This function implements a binary search algorithm specifically designed for character code conversion between Big5 (Traditional Chinese) and CNS 11643-1992 (Chinese National Standard) encodings. The function searches through a sorted array of code mappings to find the range containing the input code and then calculates the corresponding converted code point.

The function handles two conversion directions:
1. **Big5 to CNS**: For codes >= 0xa140U, it performs Big5 to CNS conversion using Big5's unique radix system (0x9d) and handles the two-region structure of Big5 encoding
2. **CNS to Big5**: For codes < 0xa140U, it performs CNS to Big5 conversion using ISO charset's radix system (0x5e)

The algorithm includes sophisticated distance calculations that account for the different encoding structures and byte ranges of each character set.

## Parameters / Member Variables
- : Pointer to a sorted array of codes_t structures containing code mappings
- : The upper bound index for the binary search (array size - 1)
- : The input character code to be converted

## Dependencies
- Functions called/Symbols referenced:
  - codes_t (structure type for code mappings)
- Called from (representative examples):
  - BIG5toCNS (at line 310 and 331)
  - CNStoBIG5 (at line 355 and 358)

## Notes and Other Information
- The function uses complex mathematical calculations to handle the different radix systems of Big5 (0x9d) and CNS (0x5e)
- Big5 encoding has two distinct byte regions with a bias adjustment (-0x22) between them
- Returns 0 if no valid mapping is found or if the peer value is 0
- The algorithm assumes the input array is properly sorted by the code field
- Critical for proper character encoding conversion in PostgreSQL's multi-byte character support