# ExecFindRowMark

## Location
[src/backend/executor/execMain.c:2379-2401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L2379-L2401)

## Overview
Retrieves the ExecRowMark structure associated with a given range table index, providing access to row locking information for a specific relation in the query.

## Definition
ExecRowMark *ExecFindRowMark(EState *estate, Index rti, bool missing_ok)

## Detailed Description
ExecFindRowMark searches for the ExecRowMark structure corresponding to a specific range table index (rti) within the execution state. The ExecRowMark structure contains information about row locking requirements for a particular relation in the query. The function performs bounds checking to ensure the range table index is valid and within the allocated array size. If the requested ExecRowMark is not found, the function's behavior depends on the missing_ok parameter - it either returns NULL or throws an error.

## Parameters / Member Variables
- `estate`: Execution state containing the es_rowmarks array and range table size information
- `rti`: Range table index (1-based) identifying the specific relation whose ExecRowMark is requested
- `missing_ok`: Boolean flag controlling error handling when the ExecRowMark is not found (true = return NULL, false = throw error)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecRowMark](ExecRowMark.md) (structure type)
  - elog (for error reporting)
- Called from (representative examples):
  - [ExecInitLockRows](ExecInitLockRows.md)
  - [ExecInitModifyTable](ExecInitModifyTable.md)

## Notes and Other Information
This function is part of PostgreSQL's row locking infrastructure, used during query execution to manage concurrent access to rows. The range table index is 1-based in PostgreSQL's range table system, but the es_rowmarks array is 0-based, hence the `rti - 1` indexing. The function includes safety checks to prevent array bounds violations when accessing the es_rowmarks array.

## Simplified Source

```c
ExecRowMark *
ExecFindRowMark(EState *estate, Index rti, bool missing_ok)
{
    // Validate range table index and check if rowmarks array exists
    if (rti > 0 && rti <= estate->es_range_table_size &&
        estate->es_rowmarks != NULL) {

        // Get the ExecRowMark for this range table entry (convert to 0-based index)
        ExecRowMark *erm = estate->es_rowmarks[rti - 1];

        if (erm)
            return erm;
    }

    // Handle missing ExecRowMark based on missing_ok flag
    if (!missing_ok)
        elog(ERROR, "failed to find ExecRowMark for rangetable index %u", rti);

    return NULL;
}
```