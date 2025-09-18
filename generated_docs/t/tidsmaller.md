# tidsmaller

## Location
src/backend/utils/adt/tid.c: 248 - 256

## Overview
A PostgreSQL function that returns the smaller of two tuple identifiers (TIDs) by comparing their positions within the database.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that compares two ItemPointer values (TIDs) and returns the one that is considered "smaller" based on their positional ordering. It uses the  function to determine the ordering relationship between the two TIDs. If the first argument is less than or equal to the second argument, it returns the first; otherwise, it returns the second. This function is useful for operations that need to find the minimum TID value among a set of TIDs, such as certain database maintenance, optimization, or range operations.

## Parameters / Member Variables
- Function uses  macro to access arguments:
  - First argument (): ItemPointer - the first TID to compare
  - Second argument (): ItemPointer - the second TID to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ITEMPOINTER (macro for extracting ItemPointer arguments)
  - ItemPointerCompare (performs the actual TID comparison)
  - PG_RETURN_ITEMPOINTER (macro for returning ItemPointer result)
- Called from:
  - No direct references found (likely accessible through SQL as a built-in function)

## Notes and Other Information
- This function implements a "min" operation for TID values
- The comparison is based on block number first, then offset within the block
- Returns the actual ItemPointer value, not just a comparison result
- Complements the  function to provide min/max operations for TIDs
- Part of PostgreSQL's TID data type operator family
- Located in src/backend/utils/adt/tid.c:248-256