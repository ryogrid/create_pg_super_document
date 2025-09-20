# range_deparse

## Location
[src/backend/utils/adt/rangetypes.c:2507-2536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2507-L2536)

## Overview
Converts a deserialized range value to its textual representation, formatting the range bounds and flags into a human-readable string format.

## Definition

```c
static char *
range_deparse(char flags, const char *lbound_str, const char *ubound_str)
```
## Detailed Description
The  function takes the internal representation of a range (flags and bound strings) and converts it into the standard PostgreSQL range text format. It handles empty ranges, bound inclusion/exclusion markers, and proper escaping of bound values. The function constructs a string using brackets/parentheses to indicate inclusive/exclusive bounds and commas to separate lower and upper bounds.

The function returns a palloc'd string that represents the range in standard PostgreSQL range syntax, such as '[1,10)' for a range from 1 (inclusive) to 10 (exclusive).

## Parameters / Member Variables
- : A byte containing range property flags (empty, bound inclusion, bound existence)
- : The lower bound value already converted to text (NULL if no lower bound)
- : The upper bound value already converted to text (NULL if no upper bound)

## Dependencies
- Functions called/Symbols referenced:
  - RANGE_EMPTY (macro for checking empty range flag)
  - RANGE_EMPTY_LITERAL (constant for empty range representation)
  - RANGE_LB_INC (macro for lower bound inclusive flag)
  - RANGE_HAS_LBOUND (macro for checking lower bound existence)
  - [range_bound_escape](range_bound_escape.md) (function for escaping bound strings)
  - RANGE_HAS_UBOUND (macro for checking upper bound existence)
  - RANGE_UB_INC (macro for upper bound inclusive flag)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication function)
  - initStringInfo, appendStringInfoChar, appendStringInfoString (string building functions)
- Called from (representative examples):
  - [range_out](range_out.md)

## Notes and Other Information
- This is a static function internal to the rangetypes.c module
- Uses StringInfo for efficient string building
- Handles all range types including empty ranges, unbounded ranges, and fully bounded ranges
- The result string follows PostgreSQL's standard range notation with '[' and ']' for inclusive bounds, '(' and ')' for exclusive bounds
- Caller is responsible for managing the memory of the returned palloc'd string