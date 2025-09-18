# uniqueWORD

## Location
[src/backend/tsearch/to_tsany.c:77-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L77-L164)

## Overview
A static function that sorts an array of ParsedWord structures, removes duplicates, and consolidates position information for identical words into position arrays.

## Definition


## Detailed Description
 performs deduplication and position consolidation on an array of ParsedWord structures. The function implements the following algorithm:

1. **Special case handling**: For single-word arrays, it simply converts the position to an array format and returns.

2. **Sorting**: Uses  with the  comparison function to sort all words lexicographically, with position as a secondary sort key.

3. **Deduplication and position consolidation**: Iterates through the sorted array and:
   - For unique words: Creates a new entry with its position stored in an array
   - For duplicate words: Consolidates the position information into the existing word's position array, subject to limits

4. **Position array management**: Each unique word maintains a dynamic array of positions where:
   -  stores the count of positions
   -  through  store the actual position values
   - Arrays are grown as needed using 

The function enforces several limits: maximum number of positions per word (), maximum position value (), and ensures position uniqueness within each word's array.

## Parameters / Member Variables
- : Array of ParsedWord structures to process
- : Length of the input array (number of elements)

Returns: Number of unique words in the resulting deduplicated array

## Dependencies
- Functions called/Symbols referenced:
  - : Comparison function for sorting ParsedWord structures
  - : Standard C library sorting function
  - : PostgreSQL memory allocation function
  - : PostgreSQL memory reallocation function
  - : PostgreSQL memory deallocation function
  - : Standard C string comparison function
  - : Macro to limit position values
  - : Maximum number of positions allowed per word
  - : Maximum position value allowed
- Called from (representative examples):
  - : Uses this function to deduplicate words before creating tsvector

## Notes and Other Information
- This is a static function internal to 
- Critical component of tsvector creation, ensuring each word appears only once with all its positions
- The position array format follows PostgreSQL's tsvector internal representation
- Memory management is handled through PostgreSQL's memory context system
- Position limits prevent excessive memory usage and maintain compatibility with tsvector storage format
- Located at lines 77-164 in 
- The function modifies the input array in-place, compacting unique results at the beginning