# CheckValidRowMarkRel

## Location
[src/backend/executor/execMain.c:1131-1195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1131-L1195)

## Overview
Validates that a proposed rowmark target relation is a legal target for row locking operations, checking relation types and FDW capabilities to ensure proper access control.

## Definition

```c
static void
CheckValidRowMarkRel(Relation rel, RowMarkType markType)
```
## Detailed Description
CheckValidRowMarkRel performs runtime validation of row marking (locking) operations on different types of relations. While the parser and planner catch most invalid cases, this function provides a final safety check during execution. It examines the relation's kind and determines whether the specified row mark type is permitted, with special handling for foreign tables that require FDW support for row refetching.

## Parameters / Member Variables
- `rel`: The target relation to validate for row marking operations
- `markType`: The type of row marking being attempted (e.g., ROW_MARK_REFERENCE, explicit locking clauses)
## Dependencies
- Functions called/Symbols referenced:
  - [GetFdwRoutineForRelation](../G/GetFdwRoutineForRelation.md)
  - RelationGetRelationName
  - ereport/errcode/errmsg (error reporting)
- Called from (representative examples):
  - [InitPlan](../I/InitPlan.md) (src/backend/executor/execMain.c:893)

## Notes and Other Information
- Allows regular tables and partitioned tables without restriction
- Prohibits row locking on sequences (not vacuumed), TOAST relations, and views
- Materialized views allow ROW_MARK_REFERENCE but not explicit locking clauses  
- Foreign tables require FDW support via RefetchForeignRow callback
- This is primarily a defensive check since parser/planner should catch most violations
- Part of the executor's initialization phase to ensure safe row marking operations

## Simplified Source

```c
static void
CheckValidRowMarkRel(Relation rel, RowMarkType markType)
{
    switch (rel->rd_rel->relkind)
    {
        case RELKIND_RELATION:
        case RELKIND_PARTITIONED_TABLE:
            // Regular tables and partitioned tables are OK
            break;

        case RELKIND_SEQUENCE:
            ereport(ERROR, "cannot lock rows in sequence");
            break;

        case RELKIND_TOASTVALUE:
            ereport(ERROR, "cannot lock rows in TOAST relation");
            break;

        case RELKIND_VIEW:
            ereport(ERROR, "cannot lock rows in view");
            break;

        case RELKIND_MATVIEW:
            // Materialized views allow references but not locking
            if (markType != ROW_MARK_REFERENCE)
                ereport(ERROR, "cannot lock rows in materialized view");
            break;

        case RELKIND_FOREIGN_TABLE:
            // Check if FDW supports row refetching
            FdwRoutine *fdwroutine = GetFdwRoutineForRelation(rel, false);
            if (fdwroutine->RefetchForeignRow == NULL)
                ereport(ERROR, "cannot lock rows in foreign table");
            break;

        default:
            ereport(ERROR, "cannot lock rows in relation");
            break;
    }
}
```