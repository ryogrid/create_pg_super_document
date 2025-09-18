# refresh_by_heap_swap

## Location
[src/backend/commands/matview.c:888-897](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/matview.c#L888-L897)

## Overview
Performs a simple materialized view refresh by swapping the physical heap files between the target materialized view and a transient table containing new data.

## Definition


## Detailed Description
This function implements the simpler of two materialized view refresh strategies in PostgreSQL. Unlike refresh_by_match_merge, this approach does not allow concurrent reads during the refresh operation. Instead, it:

1. **Direct heap swap**: Physically swaps the storage files between the existing materialized view and the temporary table containing new data
2. **Index rebuilding**: Automatically rebuilds all indexes on the materialized view to match the new data
3. **Cleanup**: Removes the transient table after the swap is complete
4. **Atomic operation**: The swap operation is atomic, ensuring consistency but blocking concurrent access

This method is typically used when concurrent access is not required or when the materialized view lacks the unique indexes necessary for the more sophisticated refresh_by_match_merge approach.

## Parameters / Member Variables
- : Object ID of the materialized view to refresh
- : Object ID of the transient table containing the new data to swap in
- : Persistence characteristic of the relation (permanent, temporary, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [finish_heap_swap](../f/finish_heap_swap.md)
  - [ReadNextMultiXactId](../R/ReadNextMultiXactId.md)
- Called from (representative examples):
  - [RefreshMatViewByOid](../R/RefreshMatViewByOid.md)

## Notes and Other Information
- This is a simpler but more disruptive refresh method compared to refresh_by_match_merge
- Does not require unique indexes on the materialized view
- Blocks all access to the materialized view during the refresh operation
- Security context switching is handled by the finish_heap_swap function
- Uses current transaction's RecentXmin and next MultiXactId for proper transaction visibility