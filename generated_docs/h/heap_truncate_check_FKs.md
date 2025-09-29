# heap_truncate_check_FKs

## Location
[src/backend/catalog/heap.c:3154-3248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L3154-L3248)

## Overview
heap_truncate_check_FKs validates that relations to be truncated do not have foreign key constraints that would be violated by the truncation operation.

## Definition
void heap_truncate_check_FKs(List *relations, bool tempTables)

## Detailed Description
This function enforces foreign key constraint validation before allowing truncation operations to proceed. It scans the list of relations to be truncated and identifies any foreign key dependencies that would prevent truncation. The function allows self-referential foreign keys but disallows foreign keys from external tables since TRUNCATE is designed to avoid scanning individual rows.

The function operates in multiple phases: first building a list of relation OIDs that have triggers or are partitioned tables (since these can have foreign keys), then performing a fast check using heap_truncate_find_FKs. If violations are found, it performs detailed scans to identify specific constraint violations and generates appropriate error messages based on whether temporary tables are involved.

## Parameters / Member Variables
- : List of Relation structures to be truncated
- : Boolean flag indicating if these are temporary tables (affects error message selection)

## Dependencies
- Functions called/Symbols referenced:
  - lfirst
  - RelationGetRelid
  - [lappend_oid](../l/lappend_oid.md)
  - [heap_truncate_find_FKs](heap_truncate_find_FKs.md)
  - lfirst_oid
  - list_make1_oid
  - [list_member_oid](../l/list_member_oid.md)
  - [get_rel_name](../g/get_rel_name.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errdetail](../e/errdetail.md)
  - [errhint](../e/errhint.md)
- Called from (representative examples):
  - [heap_truncate](heap_truncate.md)
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)

## Notes and Other Information
- Shared by both transaction-safe and non-transaction-safe truncate implementations
- Relations without triggers are skipped since they cannot have foreign keys (except partitioned tables)
- Self-referential foreign keys are allowed since they don't prevent truncation
- Provides different error messages for temporary vs. permanent tables
- Performance optimization: fast path returns early if no relations have triggers
- Uses two-phase scanning: bulk check first, then detailed analysis for error reporting
- The function assumes caller already holds appropriate locks on the relations

## Simplified Source

```c
void heap_truncate_check_FKs(List *relations, bool tempTables) {
    List *oids = NIL;

    // Build list of relation OIDs that can have foreign keys
    foreach(cell, relations) {
        Relation rel = lfirst(cell);

        // Include relations with triggers or partitioned tables
        if (rel->rd_rel->relhastriggers ||
            rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
            oids = lappend_oid(oids, RelationGetRelid(rel));
        }
    }

    // Fast path: no triggers means no foreign keys
    if (oids == NIL)
        return;

    // Check for foreign key dependencies
    List *dependents = heap_truncate_find_FKs(oids);
    if (dependents == NIL)
        return;

    // Find specific constraint violations to report
    foreach(cell, oids) {
        Oid relid = lfirst_oid(cell);
        dependents = heap_truncate_find_FKs(list_make1_oid(relid));

        foreach(cell2, dependents) {
            Oid relid2 = lfirst_oid(cell2);

            // Error if external table references this relation
            if (!list_member_oid(oids, relid2)) {
                char *relname = get_rel_name(relid);
                char *relname2 = get_rel_name(relid2);

                if (tempTables) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("unsupported ON COMMIT and foreign key combination")));
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("cannot truncate a table referenced in a foreign key constraint"),
                         errdetail("Table \"%s\" references \"%s\".", relname2, relname),
                         errhint("Truncate table \"%s\" at the same time, or use TRUNCATE ... CASCADE.", relname2)));
                }
            }
        }
    }
}
```