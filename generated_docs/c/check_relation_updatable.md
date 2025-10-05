# check_relation_updatable

## Location
[src/backend/replication/logical/worker.c:2485-2525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L2485-L2525)

## Overview
Validates that a logical replication target relation is updatable by checking for proper replica identity or primary key configuration and provides detailed error messages when requirements are not met.

## Definition
```c
static void check_relation_updatable(LogicalRepRelMapEntry *rel)
```

## Detailed Description
This function serves as a critical validation checkpoint for logical replication operations that require tuple identification (UPDATE and DELETE operations). It ensures that the target relation has the necessary infrastructure to uniquely identify tuples for modification or deletion.

The function implements a tiered validation approach:

1. **Partitioned Table Bypass**: For partitioned tables, the function returns early as updateability is determined at the individual partition level
2. **Quick Updatable Check**: If the relation is already marked as updatable in the relation mapping, no further validation is needed
3. **Detailed Analysis**: When the relation appears non-updatable, it performs a deeper analysis to provide specific error messages based on the actual configuration issue

The function provides two distinct error scenarios:
- When a replica identity or primary key exists but the publisher didn't send the required columns
- When the relation lacks both replica identity and primary key infrastructure entirely

This validation is essential for maintaining data consistency in logical replication by preventing operations that could result in incorrect or incomplete tuple identification.

## Parameters / Member Variables
- `rel`: LogicalRepRelMapEntry structure containing information about the replicated relation mapping, including local and remote relation details

## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid (macro)
  - [GetRelationIdentityOrPK](../G/GetRelationIdentityOrPK.md)
  - ereport (macro)
  - [errcode](../e/errcode.md) (macro)
  - [errmsg](../e/errmsg.md) (macro)
  - [LogicalRepRelMapEntry](../L/LogicalRepRelMapEntry.md) (data structure)
  - RELKIND_PARTITIONED_TABLE (constant)
  - ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE (constant)
- Called from (representative examples):
  - [apply_handle_update](../a/apply_handle_update.md)
  - [apply_handle_delete](../a/apply_handle_delete.md)
  - [apply_handle_tuple_routing](../a/apply_handle_tuple_routing.md)

## Notes and Other Information
- This is a static function within the logical replication worker module
- Only called for UPDATE and DELETE operations where tuple identification is required
- Provides user-friendly error messages that include both local and remote relation names
- Implements performance optimization by checking the cached updatable flag before expensive validation
- Critical for preventing data corruption in logical replication scenarios
- Part of PostgreSQL's replica identity enforcement mechanism
- The function intentionally uses slower validation in error cases to provide more accurate diagnostics
- Handles the distinction between missing replica identity configuration and missing replica identity data

## Simplified Source

```c
static void
check_relation_updatable(LogicalRepRelMapEntry *rel)
{
    // Skip check for partitioned tables
    if (rel->localrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        return;

    // If already marked as updatable, no need to check further
    if (rel->updatable)
        return;

    // Check if relation has replica identity or primary key
    if (OidIsValid(GetRelationIdentityOrPK(rel->localrel))) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("publisher did not send replica identity column "
                        "expected by the logical replication target relation \"%s.%s\"",
                        rel->remoterel.nspname, rel->remoterel.relname)));
    }

    // No replica identity or primary key found
    ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
             errmsg("logical replication target relation \"%s.%s\" has "
                    "neither REPLICA IDENTITY index nor PRIMARY "
                    "KEY and published relation does not have "
                    "REPLICA IDENTITY FULL",
                    rel->remoterel.nspname, rel->remoterel.relname)));
}
```