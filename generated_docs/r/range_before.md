# range_before

## Location
src/backend/utils/adt/rangetypes.c: 689 - 701

## Overview
The  function determines whether one range is strictly positioned before another range, implementing the PostgreSQL range "strictly left of" operator (<<).

## Definition


## Detailed Description
This function implements the range "strictly left of" comparison in PostgreSQL's range type system. It takes two range arguments and returns a boolean value indicating whether the first range is entirely before (to the left of) the second range with no overlap. The function serves as the SQL-callable wrapper for the internal  function, handling the PostgreSQL function call protocol and type cache management.

A range is considered "before" another if there is a clear gap between them, with the first range's upper boundary being less than the second range's lower boundary. This ensures strict ordering with no overlap or adjacency.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : The first range (potential "before" range) - accessed via 
  - : The second range (potential "after" range) - accessed via 

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts range arguments from function call
  -  - Retrieves type cache information for range operations
  -  - Gets the OID of the range type
  -  - Performs the actual "before" comparison logic
  -  - Returns boolean result following PostgreSQL conventions
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator <<)

## Notes and Other Information
- This function is typically invoked through the PostgreSQL SQL operator  for range "strictly left of" comparisons
- The actual comparison logic is delegated to  which handles the detailed bound comparisons
- Uses PostgreSQL's type cache system for efficient type-specific operations
- Empty ranges are handled specially by the internal function (neither before nor after any range)
- Located in 