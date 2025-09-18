# range_contains

## Location
src/backend/utils/adt/rangetypes.c: 638 - 650

## Overview
The  function determines whether one range completely contains another range, implementing the PostgreSQL range containment operator (@>).

## Definition


## Detailed Description
This function implements the range containment check in PostgreSQL's range type system. It takes two range arguments and returns a boolean value indicating whether the first range completely contains the second range. The function serves as the SQL-callable wrapper for the internal  function, handling the PostgreSQL function call protocol and type cache management.

The containment relationship means that every element that belongs to the second range also belongs to the first range. This includes cases where the ranges are identical (a range contains itself).

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : The first range (potential container) - accessed via   
  - : The second range (potential containee) - accessed via 

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts range arguments from function call
  -  - Retrieves type cache information for range operations
  -  - Gets the OID of the range type
  -  - Performs the actual containment logic
  -  - Returns boolean result following PostgreSQL conventions
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator @>)

## Notes and Other Information
- This function is typically invoked through the PostgreSQL SQL operator  for range containment
- The actual containment logic is delegated to  which handles the detailed comparison
- Uses PostgreSQL's type cache system for efficient type-specific operations
- Located in 