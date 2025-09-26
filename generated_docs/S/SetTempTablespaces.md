# SetTempTablespaces

## Location
[src/backend/storage/file/fd.c:3046-3074](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3046-L3074)

## Overview
Sets up an array of tablespace OIDs to be used for temporary files during the current transaction, with randomized starting point for load distribution.

## Definition
```c
void SetTempTablespaces(Oid *tableSpaces, int numSpaces)
```

## Detailed Description
The `SetTempTablespaces` function configures the list of tablespaces that PostgreSQL will use for creating temporary files. This function is crucial for managing storage distribution and performance optimization by allowing temporary files to be spread across multiple tablespaces.

The function sets global variables `tempTableSpaces` and `numTempTableSpaces` that will be referenced throughout the transaction. A key feature is the randomized selection of a starting point in the tablespace list using `pg_prng_uint64_range`, which helps distribute temporary files across different tablespaces when multiple backends are using the same tablespace configuration.

The tablespace array may contain `InvalidOid` entries, which indicate that the current database's default tablespace should be used for those positions. The function ensures that large temporary operations (like sorts) will cycle through all available tablespaces, promoting even distribution of I/O load.

## Parameters / Member Variables
- `tableSpaces`: Pointer to an array of tablespace OIDs to use for temporary files. May contain `InvalidOid` entries for default tablespace usage
- `numSpaces`: Number of entries in the tableSpaces array. Must be >= 0

## Dependencies
- Functions called/Symbols referenced:
  - `pg_prng_uint64_range` - Generates random number for starting position selection
  - `pg_global_prng_state` (global variable) - Global pseudo-random number generator state

- Global variables modified:
  - `tempTableSpaces` - Stores the pointer to the tablespace array
  - `numTempTableSpaces` - Stores the count of tablespaces
  - `nextTempTableSpace` - Stores the index of the next tablespace to use

- Called from (representative examples):
  - `assign_temp_tablespaces` (src/backend/commands/tablespace.c:1318, 1320)
  - `PrepareTempTablespaces` (src/backend/commands/tablespace.c:1360, 1412)

## Notes and Other Information
- The caller is responsible for ensuring the tableSpaces array has adequate lifespan, typically allocated in TopTransactionContext
- The configuration remains active until the end of the transaction or until this function is called again
- Random starting point selection minimizes conflicts between concurrent backends using the same tablespace list
- Circular advancement through the list ensures even distribution of large temporary files
- InvalidOid entries in the array are handled gracefully by falling back to the default tablespace