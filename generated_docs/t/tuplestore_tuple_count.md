# tuplestore_tuple_count

## Location
[src/backend/utils/sort/tuplestore.c:546-556](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L546-L556)

## Overview
Returns the total number of tuples that have been added to the tuplestore since its creation or the last clear operation.

## Definition


## Detailed Description
This function provides a simple accessor to retrieve the count of tuples stored in the tuplestore. The count represents the total number of tuples added via  or similar functions since the tuplestore was created or last cleared with . The count is maintained internally in the  field and includes both tuples currently in memory and those that may have been written to temporary files.

## Parameters / Member Variables
- : Pointer to the  structure whose tuple count is requested

## Dependencies
- Functions called/Symbols referenced:
  - Uses only the  field from  structure
- Called from (representative examples):
  -  (spi.c:3379, 3396)
  -  (tablesync.c:929)

## Notes and Other Information
- Returns an  value to handle large tuple counts
- The count is cumulative and does not decrease when tuples are read
- Count persists across different tuplestore states (in-memory, file-based)
- Simple O(1) operation that just returns a stored counter value
- Useful for statistics, capacity planning, and debugging
- Count resets to 0 only when  is called or a new tuplestore is created