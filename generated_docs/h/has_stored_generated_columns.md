# has_stored_generated_columns

## Location
[src/backend/optimizer/util/plancat.c:2344-2370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L2344-L2370)

## Overview
Determines whether a relation identified by a range table index has any stored generated columns, which is used during query planning to handle generated column requirements.

## Definition

```c
bool
has_stored_generated_columns(PlannerInfo *root, Index rti)
```
## Detailed Description
This function checks if a table has any stored generated columns by examining the relation's tuple descriptor constraints. Stored generated columns are columns whose values are automatically computed and physically stored based on expressions involving other columns in the same row. The function opens the relation, retrieves its tuple descriptor, and checks the constraint information for the presence of stored generated columns.

The check is performed by examining the `has_generated_stored` flag in the relation's constraint structure, which is set during table creation or alteration when stored generated columns are defined.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and information
- `rti`: Range table index identifying the relation to check for stored generated columns

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [table_open](../t/table_open.md)
  - [table_close](../t/table_close.md)
  - RelationGetDescr
- Called from (representative examples):
  - [make_modifytable](../m/make_modifytable.md)

## Notes and Other Information
- The function assumes adequate locking has already been acquired for the relation
- Used during query planning to determine if special handling is needed for stored generated columns in DML operations
- The check is efficient as it only examines metadata rather than scanning actual column definitions
- Part of the query planner's catalog utilities for handling generated column semantics

## Simplified Source

```c
bool
has_stored_generated_columns(PlannerInfo *root, Index rti)
{
    // Get the range table entry and open the relation
    RangeTblEntry *rte = planner_rt_fetch(rti, root);
    Relation relation = table_open(rte->relid, NoLock);

    // Get the tuple descriptor and check for stored generated columns
    TupleDesc tupdesc = RelationGetDescr(relation);
    bool result = tupdesc->constr && tupdesc->constr->has_generated_stored;

    // Clean up and return result
    table_close(relation, NoLock);
    return result;
}
```