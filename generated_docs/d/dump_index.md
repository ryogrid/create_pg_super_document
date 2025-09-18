# dump_index

## Location
src/backend/utils/adt/formatting.c: 2290 - 2320

## Overview
A debugging utility function that displays the contents of a KeyWord index array, showing ASCII characters and their corresponding keyword mappings.

## Definition
```c
static void dump_index(const KeyWord *k, const int *index)
```

## Detailed Description
This function is a debugging tool used to visualize how the KeyWord index mapping works in PostgreSQL's formatting system. It iterates through the KeyWord_INDEX_SIZE array and displays each ASCII character (starting from character 32, which is space) along with its corresponding keyword name if mapped, or indicates if the position is free. The function provides detailed statistics about used and free positions in the index array, helping developers understand and debug the keyword lookup mechanism.

## Parameters / Member Variables
- `k`: Pointer to an array of KeyWord structures containing the keyword definitions
- `index`: Pointer to an integer array that maps ASCII character positions to KeyWord array indices

## Dependencies
- Functions called/Symbols referenced:
  - elog (for debug output)
  - KeyWord (structure type)
  - DEBUG_elog_output (debug level constant)
  - KeyWord_INDEX_SIZE (size constant for the index array)
- Called from (representative examples):
  - DCH_ZONED
  - [NUM_cache](../N/NUM_cache.md)

## Notes and Other Information
- This is a static function local to src/backend/utils/adt/formatting.c
- Only active when debug logging is enabled (uses DEBUG_elog_output level)
- The index starts at ASCII character 32 (space) since the first 32 control characters are skipped
- Used for debugging both DATE-TIME (DCH) and NUMBER (NUM) formatting keyword systems
- Provides valuable insight into the character-to-keyword mapping efficiency and collision detection
- Index entries with value -1 indicate unused/free positions