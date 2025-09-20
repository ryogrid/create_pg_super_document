# text_pattern_lt

## Location
[src/backend/utils/adt/varlena.c:2819-2834](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2819-L2834)

## Overview
PostgreSQL function that performs character-by-character "less than" comparison of two text values, designed specifically for pattern matching and LIKE clause operations.

## Definition

```c
Datum
text_pattern_lt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that compares two text values using character-by-character (byte-level) comparison and returns true if the first text value is lexicographically less than the second. This function is part of PostgreSQL's pattern matching infrastructure and is specifically designed to support efficient indexing for LIKE clause operations.

The function extracts two text arguments using PostgreSQL's function call interface, delegates the actual comparison to , and returns a boolean result. It also properly manages memory by freeing copied arguments when necessary.

## Parameters / Member Variables
- Uses  macro to access function arguments:
  - First argument: text value extracted via 
  - Second argument: text value extracted via 

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to extract text arguments from function call
  -  - Core comparison function for pattern operations
  -  - Macro to free copied arguments if necessary
  -  - Macro to return boolean Datum
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator framework)

## Notes and Other Information
- This function is part of PostgreSQL's operator framework for pattern matching operations
- The function is defined in  at lines 2819-2834
- Uses character-by-character comparison rather than locale-aware string comparison
- Designed to be compatible with indexes built for LIKE clause optimization
- Returns true when the first text argument is lexicographically less than the second
- Properly handles PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) by using  and 
- Part of a family of pattern comparison functions including , , and 