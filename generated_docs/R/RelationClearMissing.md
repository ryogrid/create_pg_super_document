# RelationClearMissing

## Location
[src/backend/catalog/heap.c:1947-2012](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L1947-L2012)

## Overview
RelationClearMissing clears the missing value information (atthasmissing and attmissingval) for all attributes of a relation, used when the table is rewritten and no longer needs missing value defaults.

## Definition

```c
void
RelationClearMissing(Relation rel)
```
## Detailed Description
This function removes missing value information from all attributes in a relation by setting atthasmissing to false and attmissingval to null in pg_attribute. It is safely used when a table is completely rewritten (such as by VACUUM FULL or CLUSTER) where all rows are guaranteed to have the full complement of attributes, making missing value defaults unnecessary. The function iterates through all non-system attributes, finds those with atthasmissing set to true, and updates their pg_attribute entries to clear the missing value information. This optimization reduces storage overhead and eliminates unnecessary missing value processing.

## Parameters / Member Variables
- `rel`: Relation object for which to clear missing value information (caller must hold AccessExclusive lock)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfAttributes
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [finish_heap_swap](../f/finish_heap_swap.md)
  - [ATExecSetExpression](../A/ATExecSetExpression.md)
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md)

## Notes and Other Information
- Requires AccessExclusive lock on the relation (must be held by caller)
- Only processes attributes where atthasmissing is currently true
- Sets atthasmissing to false and attmissingval to null for applicable attributes
- Processes all non-system attributes including dropped columns
- Triggers automatic relcache rebuild when pg_attribute rows are updated
- Commonly used after table rewrites (VACUUM FULL, CLUSTER, ALTER COLUMN TYPE)
- Safe to call when all table rows have full attribute complement
- Improves performance by eliminating unnecessary missing value processing
- Uses heap_modify_tuple to update pg_attribute entries efficiently

## Simplified Source

```c
void
RelationClearMissing(Relation rel)
{
    Relation attr_rel;
    Oid relid = RelationGetRelid(rel);
    int natts = RelationGetNumberOfAttributes(rel);
    int attnum;
    Datum repl_val[Natts_pg_attribute];
    bool repl_null[Natts_pg_attribute];
    bool repl_repl[Natts_pg_attribute];
    Form_pg_attribute attrtuple;
    HeapTuple tuple, newtuple;

    // Initialize arrays for tuple modification
    memset(repl_val, 0, sizeof(repl_val));
    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));

    // Set values to clear missing information
    repl_val[Anum_pg_attribute_atthasmissing - 1] = BoolGetDatum(false);
    repl_null[Anum_pg_attribute_attmissingval - 1] = true;
    repl_repl[Anum_pg_attribute_atthasmissing - 1] = true;
    repl_repl[Anum_pg_attribute_attmissingval - 1] = true;

    // Open pg_attribute for updates
    attr_rel = table_open(AttributeRelationId, RowExclusiveLock);

    // Process each attribute of the relation
    for (attnum = 1; attnum <= natts; attnum++) {
        // Get attribute tuple from system cache
        tuple = SearchSysCache2(ATTNUM,
                               ObjectIdGetDatum(relid),
                               Int16GetDatum(attnum));
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "cache lookup failed for attribute %d of relation %u",
                 attnum, relid);

        attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);

        // Only update attributes that currently have missing values
        if (attrtuple->atthasmissing) {
            // Create modified tuple with cleared missing info
            newtuple = heap_modify_tuple(tuple, RelationGetDescr(attr_rel),
                                        repl_val, repl_null, repl_repl);

            // Update the catalog
            CatalogTupleUpdate(attr_rel, &newtuple->t_self, newtuple);
            heap_freetuple(newtuple);
        }

        ReleaseSysCache(tuple);
    }

    // Close pg_attribute - this will trigger relcache rebuild
    table_close(attr_rel, RowExclusiveLock);
}
```