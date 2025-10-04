# PLy_procedure_valid

## Location
[src/pl/plpython/plpy_procedure.c:415-428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_procedure.c#L415-L428)

## Overview
PLy_procedure_valid is a static validation function that determines whether a cached PLyProcedure struct is still valid by comparing it against the current pg_proc catalog tuple.

## Definition
```c
static bool PLy_procedure_valid(PLyProcedure *proc, HeapTuple procTup)
```

## Detailed Description
This function implements cache validation logic for PL/Python procedures by checking if a cached procedure definition is still current. It performs two key validations: first, it ensures the procedure pointer is not NULL, and second, it verifies that the cached procedure's metadata (transaction ID and tuple identifier) matches the current pg_proc catalog entry. This validation is essential for maintaining consistency between cached procedure definitions and their corresponding catalog entries, ensuring that procedures are recompiled when their definitions change.

## Parameters / Member Variables
- `proc`: Pointer to the cached PLyProcedure structure to validate
- `procTup`: HeapTuple representing the current pg_proc catalog entry for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [PLyProcedure](PLyProcedure.md) (structure type)
  - HeapTupleHeaderGetRawXmin (PostgreSQL tuple header function)
  - [ItemPointerEquals](../I/ItemPointerEquals.md) (PostgreSQL tuple identifier comparison)
- Called from (representative examples):
  - [PLy_procedure_get](PLy_procedure_get.md)

## Notes and Other Information
- This is a static function, only accessible within the plpy_procedure.c file
- Uses PostgreSQL's MVCC (Multi-Version Concurrency Control) metadata for validation
- The fn_xmin and fn_tid fields store the transaction ID and tuple identifier from when the procedure was cached
- Returns false if the procedure is NULL or if the catalog entry has been modified
- Part of PostgreSQL's procedure caching mechanism to avoid unnecessary recompilation

## Simplified Source

```c
static bool PLy_procedure_valid(PLyProcedure *proc, HeapTuple procTup) {
    // Check if procedure exists
    if (proc == NULL)
        return false;

    // Verify cached procedure matches current catalog entry
    // by comparing transaction ID and tuple identifier
    if (!(proc->fn_xmin == HeapTupleHeaderGetRawXmin(procTup->t_data) &&
          ItemPointerEquals(&proc->fn_tid, &procTup->t_self)))
        return false;

    return true;
}
```