# GetTableAmRoutine

## Location
[src/backend/access/table/tableamapi.c:28-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/tableamapi.c#L28-L104)

## Overview
GetTableAmRoutine is a PostgreSQL function that retrieves and validates the TableAmRoutine struct from a table access method handler, ensuring all required callback functions are properly defined.

## Definition

```c
struct",
			 amhandler);
```
## Detailed Description
This function serves as the central mechanism for obtaining and validating table access method routines in PostgreSQL. It calls the specified access method handler function using the provided OID to retrieve a TableAmRoutine struct. The function performs comprehensive validation by asserting that all required callback functions are present, which helps maintain consistency across different access methods and makes it easier to keep access methods up to date when porting to new PostgreSQL versions.

The function allocates the TableAmRoutine struct in the caller's memory context and performs extensive validation checks to ensure the access method implementation is complete and correct. This validation includes checking for the presence of essential callbacks for scanning, parallel operations, index fetching, tuple operations, and relation management.

## Parameters / Member Variables
- : OID of the access method handler function that will return the TableAmRoutine struct

## Dependencies
- Functions called/Symbols referenced:
  - OidFunctionCall0
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - IsA
  - elog
  - [TableAmRoutine](../T/TableAmRoutine.md) (struct type)
- Called from (representative examples):
  - [InitTableAmRoutine](../I/InitTableAmRoutine.md)
  - [table_scan_sample_next_tuple](../t/table_scan_sample_next_tuple.md)

## Notes and Other Information
- The function performs extensive assertion checks to validate that all required callbacks are present in the TableAmRoutine struct
- This validation helps ensure access method implementations are complete and helps with forward porting to new PostgreSQL versions
- The returned TableAmRoutine struct is allocated in the caller's memory context
- The function will throw an ERROR if the handler doesn't return a proper TableAmRoutine struct
- Some callbacks like scan_bitmap_next_block and scan_bitmap_next_tuple are optional but must be present together if either is implemented

## Simplified Source

```c
const TableAmRoutine *GetTableAmRoutine(Oid amhandler) {
    // Call the access method handler to get the routine struct
    Datum datum = OidFunctionCall0(amhandler);
    const TableAmRoutine *routine = (TableAmRoutine *) DatumGetPointer(datum);

    // Validate that we got a proper TableAmRoutine struct
    if (routine == NULL || !IsA(routine, TableAmRoutine))
        elog(ERROR, "table access method handler %u did not return a TableAmRoutine struct",
             amhandler);

    // Assert that all required callbacks are present
    // Basic scan operations
    Assert(routine->scan_begin != NULL);
    Assert(routine->scan_end != NULL);
    Assert(routine->scan_rescan != NULL);
    Assert(routine->scan_getnextslot != NULL);

    // Parallel scan support
    Assert(routine->parallelscan_estimate != NULL);
    Assert(routine->parallelscan_initialize != NULL);
    Assert(routine->parallelscan_reinitialize != NULL);

    // Index fetch operations
    Assert(routine->index_fetch_begin != NULL);
    Assert(routine->index_fetch_reset != NULL);
    Assert(routine->index_fetch_end != NULL);
    Assert(routine->index_fetch_tuple != NULL);

    // Tuple operations
    Assert(routine->tuple_fetch_row_version != NULL);
    Assert(routine->tuple_tid_valid != NULL);
    Assert(routine->tuple_get_latest_tid != NULL);
    Assert(routine->tuple_satisfies_snapshot != NULL);
    Assert(routine->tuple_insert != NULL);
    Assert(routine->tuple_delete != NULL);
    Assert(routine->tuple_update != NULL);
    Assert(routine->tuple_lock != NULL);

    // Relation operations
    Assert(routine->relation_set_new_filelocator != NULL);
    Assert(routine->relation_copy_data != NULL);
    Assert(routine->relation_vacuum != NULL);
    Assert(routine->relation_size != NULL);
    Assert(routine->relation_estimate_size != NULL);

    // Additional required operations (abbreviated for simplicity)
    // ... more assertions for remaining required callbacks

    return routine;
}
```