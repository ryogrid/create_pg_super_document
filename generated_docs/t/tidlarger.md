# tidlarger

## Location
[src/backend/utils/adt/tid.c:239-247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tid.c#L239-L247)

## Overview
A PostgreSQL function that returns the larger of two tuple identifiers (TIDs) by comparing their positions within the database.

## Definition

```c
struct ItemPointerData, we can at
	 * least make this code work, by not using sizeof(ItemPointerData).
	 * Instead rely on knowing the sizes of the component fields.
	 */
	return hash_any((unsigned char *) key,
					sizeof(BlockIdData) + sizeof(OffsetNumber));
```
## Detailed Description
The  function is a PostgreSQL built-in function that compares two ItemPointer values (TIDs) and returns the one that is considered "larger" based on their positional ordering. It uses the  function to determine the ordering relationship between the two TIDs. If the first argument is greater than or equal to the second argument, it returns the first; otherwise, it returns the second. This function is useful for operations that need to find the maximum TID value among a set of TIDs, such as certain database maintenance or optimization tasks.

## Parameters / Member Variables
- Function uses  macro to access arguments:
  - First argument (): ItemPointer - the first TID to compare
  - Second argument (): ItemPointer - the second TID to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ITEMPOINTER (macro for extracting ItemPointer arguments)
  - [ItemPointerCompare](../I/ItemPointerCompare.md) (performs the actual TID comparison)
  - PG_RETURN_ITEMPOINTER (macro for returning ItemPointer result)
- Called from:
  - No direct references found (likely accessible through SQL as a built-in function)

## Notes and Other Information
- This function implements a "max" operation for TID values
- The comparison is based on block number first, then offset within the block
- Returns the actual ItemPointer value, not just a comparison result
- Part of PostgreSQL's TID data type operator family
- Located in src/backend/utils/adt/tid.c:239-247