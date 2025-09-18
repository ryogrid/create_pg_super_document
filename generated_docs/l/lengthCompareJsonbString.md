# lengthCompareJsonbString

## Location
[src/backend/utils/adt/jsonb_util.c:1905-1924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1905-L1924)

## Overview
A low-level string comparison function that compares two strings by length first, then by binary content, providing the core comparison logic for JSONB string operations and binary searches.

## Definition


## Detailed Description
The  function implements the fundamental comparison logic used throughout the JSONB system for string comparisons. It uses a two-stage comparison approach:

1. **Length comparison**: If the strings have different lengths, the shorter string is considered "less than" the longer string
2. **Binary comparison**: If the strings have the same length, it performs a binary comparison using  to determine ordering

This approach provides a well-defined, consistent ordering that is optimized for performance in JSONB operations, particularly binary searches on sorted containers. The length-first strategy often eliminates the need for expensive byte-by-byte comparisons.

## Parameters / Member Variables
- : Pointer to the first string to compare
- : Length of the first string in bytes
- : Pointer to the second string to compare  
- : Length of the second string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - memcmp (standard C library function for binary memory comparison)
- Called from:
  - [lengthCompareJsonbStringValue](lengthCompareJsonbStringValue.md) (wrapper for JsonbValue string comparisons)
  - [getKeyJsonValueFromContainer](../g/getKeyJsonValueFromContainer.md) (for binary searches in JSONB containers)

## Notes and Other Information
- Returns standard comparator values: negative if val1 < val2, zero if equal, positive if val1 > val2
- Specifically designed for efficient binary searches on JSONB containers
- The length-first comparison strategy provides better performance characteristics than lexical comparison
- Does not perform null pointer checking - relies on callers to provide valid inputs
- Uses  for binary equality testing rather than string comparison functions
- Enables efficient key lookups in JSONB objects without requiring full lexical sort order
- Essential building block for JSONB container search and comparison operations
- The function is static and only used internally within the JSONB system
- Optimized for the common case where strings have different lengths