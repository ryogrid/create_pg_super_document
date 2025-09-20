# range_contained_by

## Location
[src/backend/utils/adt/rangetypes.c:651-663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L651-L663)

## Overview
The  function determines whether one range is completely contained by another range, implementing the PostgreSQL range containment operator (<@).

## Definition

```c
Datum
range_contained_by(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the range containment check from the opposite perspective of . It takes two range arguments and returns a boolean value indicating whether the first range is completely contained by the second range. The function serves as the SQL-callable wrapper for the internal  function, handling the PostgreSQL function call protocol and type cache management.

The contained-by relationship means that every element that belongs to the first range also belongs to the second range. This is the inverse relationship of  - where  checks if A contains B,  checks if A is contained by B.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : The first range (potential containee) - accessed via 
  - : The second range (potential container) - accessed via 

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts range arguments from function call
  -  - Retrieves type cache information for range operations
  -  - Gets the OID of the range type
  -  - Performs the actual contained-by logic
  -  - Returns boolean result following PostgreSQL conventions
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator <@)

## Notes and Other Information
- This function is typically invoked through the PostgreSQL SQL operator  for range contained-by
- The actual containment logic is delegated to  which handles the detailed comparison  
- Uses PostgreSQL's type cache system for efficient type-specific operations
- Provides the inverse operation to  - they are complementary functions
- Located in 