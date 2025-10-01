# allocate_record_info

## Location
[src/backend/utils/adt/jsonfuncs.c:3474-3489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3474-L3489)

## Overview
Allocates and initializes a RecordIOData structure for handling record (composite type) I/O operations with a specified number of columns.

## Definition
```c
static RecordIOData *allocate_record_info(MemoryContext mcxt, int ncolumns)
```

## Detailed Description
This function allocates memory for a RecordIOData structure along with space for the specified number of ColumnIOData elements in a single allocation. The function uses a flexible array member approach, calculating the total memory needed using offsetof to account for the variable-sized columns array. After allocation, it initializes the structure with safe defaults: InvalidOid for record_type, 0 for record_typmod, the provided column count, and zeros out all column metadata using MemSet.

## Parameters / Member Variables
- `mcxt`: Memory context in which to allocate the RecordIOData structure
- `ncolumns`: Number of columns (fields) the record type contains

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - offsetof
  - InvalidOid
  - MemSet
- Called from (representative examples):
  - JsObjectFree
  - [populate_record](../p/populate_record.md)

## Notes and Other Information
This function demonstrates efficient memory management by allocating the RecordIOData structure and its variable-length columns array in a single memory block, avoiding fragmentation and improving cache locality. The use of offsetof ensures proper alignment and portability across different architectures. The initialization to safe defaults (InvalidOid, zero typmod) ensures that the structure can be safely used and checked for validity. This allocation pattern is common in PostgreSQL for structures with variable-length trailing arrays.

## Simplified Source

```c
static RecordIOData *
allocate_record_info(MemoryContext mcxt, int ncolumns)
{
    // Allocate structure with space for variable number of columns
    RecordIOData *data = (RecordIOData *)
        MemoryContextAlloc(mcxt,
            offsetof(RecordIOData, columns) +
            ncolumns * sizeof(ColumnIOData));

    // Initialize with safe defaults
    data->record_type = InvalidOid;
    data->record_typmod = 0;
    data->ncolumns = ncolumns;
    MemSet(data->columns, 0, sizeof(ColumnIOData) * ncolumns);

    return data;
}
```