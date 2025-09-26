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