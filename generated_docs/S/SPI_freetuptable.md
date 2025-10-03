# SPI_freetuptable

## Location
[src/backend/executor/spi.c:1386-1444](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1386-L1444)

## Overview
SPI_freetuptable is a function that safely deallocates a previously allocated SPITupleTable structure, removing it from the current SPI context and freeing all associated memory.

## Definition

```c
void
SPI_freetuptable(SPITupleTable *tuptable)
```
## Detailed Description
This function provides a safe mechanism to deallocate SPITupleTable structures created by SPI query execution functions. It performs several important safety checks:

1. **Context Validation**: The function searches for the tuple table only within the topmost SPI context to ensure proper scope management.
2. **Double Deletion Protection**: It guards against attempts to delete the same tuple table multiple times by checking if the table exists in the current context before deletion.
3. **Global Variable Safety**: It resets global variables (_SPI_current->tuptable and SPI_tuptable) if they point to the table being deleted.
4. **Memory Cleanup**: It completely deallocates the tuple table's memory context, freeing all associated memory.

The function uses a singly-linked list to track tuple tables within each SPI context, ensuring proper resource management and preventing memory leaks.

## Parameters / Member Variables
- : Pointer to the SPITupleTable structure to be freed. Can be NULL (function will return safely without action).

## Dependencies
- Functions called/Symbols referenced:
  - slist_foreach_modify (iterate through tuple table list)
  - slist_container (get container from list node)
  - [slist_delete_current](../s/slist_delete_current.md) (remove current item from list)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (free memory context)
  - elog (log warning messages)
- Called from (representative examples):
  - [_SPI_execute_plan](_SPI_execute_plan.md) (internal SPI execution cleanup)
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md) (text search query rewriting)
  - [ts_stat_sql](../t/ts_stat_sql.md) (text search statistics)
  - [plperl_spi_execute_fetch_result](../p/plperl_spi_execute_fetch_result.md) (Perl procedural language)
  - [PLy_spi_execute_fetch_result](../P/PLy_spi_execute_fetch_result.md) (Python procedural language)
  - [pltcl_process_SPI_result](../p/pltcl_process_SPI_result.md) (Tcl procedural language)

## Notes and Other Information
- The function gracefully handles NULL pointers by returning immediately without error.
- Double deletion attempts result in a WARNING log message rather than an error, as memory leaks are considered less severe than crashes.
- The function only searches the topmost SPI context, not nested contexts, which enforces proper scoping rules.
- This function is essential for preventing memory leaks in SPI-based applications and procedural languages.
- The tuple table's memory context deletion ensures all related memory (including tuple data) is properly freed.

## Simplified Source

```c
void SPI_freetuptable(SPITupleTable *tuptable) {
    // Handle NULL input gracefully
    if (tuptable == NULL) {
        return;
    }

    bool found = false;

    // Search for tuple table in current SPI context
    if (_SPI_current != NULL) {
        slist_mutable_iter iterator;
        slist_foreach_modify(iterator, &_SPI_current->tuptables) {
            SPITupleTable *current_table = slist_container(SPITupleTable, next, iterator.cur);

            if (current_table == tuptable) {
                slist_delete_current(&iterator);  // Remove from list
                found = true;
                break;
            }
        }
    }

    // Guard against double deletion or invalid pointers
    if (!found) {
        elog(WARNING, "attempt to delete invalid SPITupleTable %p", tuptable);
        return;
    }

    // Reset global variables that might point to this table
    if (tuptable == _SPI_current->tuptable) {
        _SPI_current->tuptable = NULL;
    }
    if (tuptable == SPI_tuptable) {
        SPI_tuptable = NULL;
    }

    // Free all memory associated with the tuple table
    MemoryContextDelete(tuptable->tuptabcxt);
}
```