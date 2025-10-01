# get_partition_qual_relid

## Location
[src/backend/utils/cache/partcache.c:299-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/partcache.c#L299-L336)

## Overview
Retrieves the partition constraint qualification for a relation specified by OID, returning it as a single boolean expression tree suitable for SQL display functions.

## Definition
```c
Expr *get_partition_qual_relid(Oid relid)
```

## Detailed Description
get_partition_qual_relid is a robust function that retrieves partition constraints for a relation identified by its OID. Unlike RelationGetPartitionQual which works with an open Relation, this function handles the complete lifecycle including relation opening, constraint generation, and proper cleanup.

The function includes comprehensive error handling for cases where the relation doesn't exist, isn't a partition, or has no partition constraints (such as when a default partition is the only partition). It converts the internal list-of-ANDed-conditions format into a proper boolean expression tree suitable for display purposes.

Key features:
- Validates that the relation exists and is a partition using get_rel_relispartition
- Opens the relation with AccessShareLock for safe access
- Generates partition qualifications using generate_partition_qual
- Converts the list format to a proper Expr tree (single expr, AND expression, or NULL)
- Maintains the lock during processing to allow safe deparsing by the caller
- Closes relation without releasing the lock (caller's responsibility)

## Parameters / Member Variables
- `relid`: The OID of the relation for which to retrieve partition constraints

## Dependencies
- Functions called/Symbols referenced:
  - [get_rel_relispartition](get_rel_relispartition.md) (validates relation is a partition)
  - [relation_open](../r/relation_open.md) (opens relation with AccessShareLock)
  - [generate_partition_qual](generate_partition_qual.md) (generates partition constraints)
  - [makeBoolExpr](../m/makeBoolExpr.md) (creates AND expression for multiple constraints)
  - [relation_close](../r/relation_close.md) (closes relation, keeping lock)
- Called from (representative examples):
  - [pg_get_partition_constraintdef](../p/pg_get_partition_constraintdef.md) (src/backend/utils/adt/ruleutils.c:2084)
  - [pg_get_partconstrdef_string](../p/pg_get_partconstrdef_string.md) (src/backend/utils/adt/ruleutils.c:2113)

## Notes and Other Information
- Returns NULL if relation doesn't exist, isn't a partition, or has no constraints
- Specifically designed to support SQL functions that may receive arbitrary OIDs
- Handles the special case where a default partition is the only partition (no constraints)
- Converts list format to expression tree: NIL->NULL, single item->item, multiple items->AND expression
- Maintains AccessShareLock through return to allow caller to safely deparse the result
- Used primarily by rule/constraint display functions in the SQL interface
- More robust than RelationGetPartitionQual for external/SQL callable functions

## Simplified Source

```c
Expr *
get_partition_qual_relid(Oid relid)
{
    Expr *result = NULL;

    // Check if relation exists and is a partition
    if (get_rel_relispartition(relid)) {
        Relation rel = relation_open(relid, AccessShareLock);
        List *and_args;

        // Generate partition constraints for this relation
        and_args = generate_partition_qual(rel);

        // Convert list format to proper boolean expression
        if (and_args == NIL)
            result = NULL;                    // No constraints
        else if (list_length(and_args) > 1)
            result = makeBoolExpr(AND_EXPR, and_args, -1);  // Multiple constraints
        else
            result = linitial(and_args);      // Single constraint

        // Close relation but keep the lock for caller safety
        relation_close(rel, NoLock);
    }

    return result;
}
```