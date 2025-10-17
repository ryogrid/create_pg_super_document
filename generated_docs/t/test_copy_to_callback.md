# test_copy_to_callback

## Location
[src/test/modules/test_copy_callbacks/test_copy_callbacks.c:34-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_copy_callbacks/test_copy_callbacks.c#L34-L51)

## Overview
A PostgreSQL function that demonstrates and tests the COPY TO callback mechanism by performing a complete COPY TO operation with a custom callback on a specified relation.

## Definition

```c
Datum
test_copy_to_callback(PG_FUNCTION_ARGS)
```
## Detailed Description
The `test_copy_to_callback` function is a PostgreSQL extension function designed to test the COPY TO callback functionality. It takes a relation OID as input, opens the specified table, and performs a complete COPY TO operation using a custom callback function (`to_cb`). 

The function demonstrates the full lifecycle of a COPY TO operation with callbacks:
1. Opens the specified relation with AccessShareLock
2. Initializes a COPY TO state with BeginCopyTo, providing the callback function
3. Executes the actual copy operation with DoCopyTo
4. Cleans up the COPY TO state with EndCopyTo
5. Reports the number of processed rows
6. Closes the relation

This function is primarily used for testing and validation of PostgreSQL's COPY TO callback infrastructure, ensuring that custom callback functions work correctly with the COPY subsystem.

## Parameters / Member Variables
- Function takes PostgreSQL function arguments via `PG_FUNCTION_ARGS` macro
- `PG_GETARG_OID(0)`: The OID of the relation (table) to perform COPY TO operation on

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](table_open.md) (to open the relation with AccessShareLock)
  - [BeginCopyTo](../B/BeginCopyTo.md) (to initialize COPY TO state with callback)
  - [to_cb](to_cb.md) (the callback function passed to BeginCopyTo)
  - [DoCopyTo](../D/DoCopyTo.md) (to execute the COPY TO operation)
  - [EndCopyTo](../E/EndCopyTo.md) (to finalize and clean up COPY TO state)
  - ereport (for logging the number of processed rows)
  - [table_close](table_close.md) (to close the relation)
  - PG_RETURN_VOID (to return from the function)
- Called from (representative examples):
  - SQL functions or test scripts that invoke this extension function

## Notes and Other Information
- This function is declared with `PG_FUNCTION_INFO_V1` macro, making it available as a PostgreSQL SQL function
- The function uses AccessShareLock when opening the relation, which is appropriate for read-only operations
- The BeginCopyTo call uses NULL for several parameters (query, filename, format options) indicating default behavior
- The callback mechanism allows monitoring and processing of data as it flows through the COPY TO operation
- Returns void (`PG_RETURN_VOID()`) as it's primarily used for its side effects (logging via callback)
- Part of the test infrastructure in `src/test/modules/test_copy_callbacks/` for validating COPY functionality
- The processed row count is reported via NOTICE message, making it visible in PostgreSQL logs or client output

## Simplified Source

```c
Datum test_copy_to_callback(PG_FUNCTION_ARGS) {
    // Open the specified relation for reading
    Relation rel = table_open(PG_GETARG_OID(0), AccessShareLock);

    // Initialize COPY TO operation with callback
    CopyToState cstate = BeginCopyTo(NULL, rel, NULL, RelationGetRelid(rel),
                                     NULL, false, to_cb, NIL, NIL);

    // Execute the COPY TO operation
    int64 processed = DoCopyTo(cstate);

    // Clean up COPY TO state
    EndCopyTo(cstate);

    // Report number of processed rows
    ereport(NOTICE, (errmsg("COPY TO callback has processed %lld rows",
                           (long long) processed)));

    // Close relation and return
    table_close(rel, NoLock);
    PG_RETURN_VOID();
}
```