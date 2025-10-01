# toast_delete_external

## Location
[src/backend/access/table/toast_helper.c:318-337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/toast_helper.c#L318-L337)

## Overview
Iterates through all attributes of a tuple and deletes any externally stored TOAST values from the secondary toast relation.

## Definition

```c
void
toast_delete_external(Relation rel, const Datum *values, const bool *isnull,
					  bool is_speculative)
```
## Detailed Description
The  function is responsible for cleaning up externally stored TOAST values when a tuple is being deleted or updated. It examines each attribute of the given relation's tuple descriptor and identifies variable-length attributes (attlen == -1) that are stored externally on disk. For each such attribute that is not null and is marked as externally stored on disk, it calls  to remove the associated TOAST chunks from the secondary toast relation.

This function is a key component of PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system, ensuring that when tuples are deleted, any associated externally stored data is properly cleaned up to prevent storage leaks.

## Parameters / Member Variables
- : The relation containing the tuple whose external TOAST values need to be deleted
- : Array of Datum values for each attribute in the tuple
- : Array of boolean flags indicating which attributes are null
- : Boolean flag indicating whether this is a speculative deletion (used for speculative insertions that may be rolled back)

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTERNAL_ONDISK (macro to check if a value is externally stored on disk)
  - [toast_delete_datum](toast_delete_datum.md) (function to delete chunks of a single externally stored TOAST value)
- Called from (representative examples):
  - [heap_toast_delete](../h/heap_toast_delete.md) (main interface for TOAST deletion in heap operations)

## Notes and Other Information
- The function only processes variable-length attributes (attlen == -1) as these are the only attributes that can be TOASTed
- Null values are skipped as they have no external storage to clean up
- The  parameter is passed through to  to handle speculative operations properly
- This function works at the tuple level, processing all potentially TOASTed attributes in a single pass
- It's part of the TOAST helper functions that provide a clean interface for TOAST operations across different table access methods

## Simplified Source

```c
void
toast_delete_external(Relation rel, const Datum *values, const bool *isnull,
                      bool is_speculative)
{
    TupleDesc tupleDesc = rel->rd_att;
    int numAttrs = tupleDesc->natts;

    // Check each attribute for external TOAST values
    for (int i = 0; i < numAttrs; i++) {
        // Only process variable-length attributes (TOAST candidates)
        if (TupleDescAttr(tupleDesc, i)->attlen == -1) {
            Datum value = values[i];

            // Skip null values - no external storage to delete
            if (isnull[i])
                continue;

            // Delete externally stored TOAST chunks
            if (VARATT_IS_EXTERNAL_ONDISK(value))
                toast_delete_datum(rel, value, is_speculative);
        }
    }
}
```