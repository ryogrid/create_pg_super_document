# namelttext

## Location
[src/backend/utils/adt/varlena.c:2738-2743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2738-L2743)

## Overview
The  function implements the less-than comparison operator between a name type and a text type in PostgreSQL.

## Definition

```c
Datum
namelttext(PG_FUNCTION_ARGS)
```
## Detailed Description
This function determines if a name (fixed-length string) is lexicographically less than a text value. It leverages the  function to perform the actual comparison and returns true if the comparison result is less than zero. This is a simple wrapper function that provides a specific boolean comparison operation based on the three-way comparison provided by .

## Parameters / Member Variables
- Inherits parameters from : name and text arguments

## Dependencies
- Functions called/Symbols referenced:
  - : Macro for calling comparison functions
  - : Three-way comparison function between name and text
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/varlena.c:2738-2743
- Simple wrapper around  for less-than comparison
- Uses  macro for efficient function invocation
- Returns boolean result: true if name < text, false otherwise
- Part of the complete set of comparison operators between name and text types

## Simplified Source

```c
Datum
namelttext(PG_FUNCTION_ARGS)
{
    // Return true if name < text comparison result is negative
    PG_RETURN_BOOL(CmpCall(btnametextcmp) < 0);
}
```