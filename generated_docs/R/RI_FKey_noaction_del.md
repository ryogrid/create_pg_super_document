# RI_FKey_noaction_del

## Location
[src/backend/utils/adt/ri_triggers.c:551-570](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L551-L570)

## Overview
Trigger function that enforces NO ACTION foreign key constraint behavior during DELETE operations on the primary key table, preventing deletions that would violate referential integrity.

## Definition

```c
Datum
RI_FKey_noaction_del(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the NO ACTION foreign key constraint action for DELETE operations on primary key tables. When a row in a primary key table is being deleted, this function checks whether any foreign key rows in referencing tables depend on the key being deleted. If dependencies exist, it prevents the deletion by raising a foreign key violation error.

The NO ACTION constraint differs from RESTRICT in its timing and behavior:
- NO ACTION checks are performed at the end of the statement, allowing other operations in the same statement to potentially resolve the constraint violation
- If another primary key row with the same key values is created in the same statement, the constraint violation is avoided

The function delegates the main constraint checking logic to the  function, passing  for the  parameter to distinguish it from RESTRICT behavior.

## Parameters / Member Variables
This function follows PostgreSQL's trigger function interface:
- Uses  macro which provides access to 
-  contains the  structure with information about the DELETE operation being performed

## Dependencies
- Functions called/Symbols referenced:
  - [ri_CheckTrigger](../r/ri_CheckTrigger.md)
  - [ri_restrict](../r/ri_restrict.md)
  - RI_TRIGTYPE_DELETE (constant)
  - [TriggerData](../T/TriggerData.md) (type cast)
- Called from (representative examples):
  - No direct references found in the codebase analysis

## Notes and Other Information
- This function is typically installed as a trigger function on primary key tables for DELETE events
- The main constraint validation logic is shared with RESTRICT through the  function
- NO ACTION constraints are checked at the end of the SQL statement, not immediately like RESTRICT
- If another primary key row is inserted with the same values during the same statement, the constraint violation is avoided
- Located in src/backend/utils/adt/ri_triggers.c:551-558
- Part of PostgreSQL's comprehensive foreign key constraint system
- Raises FOREIGN_KEY_VIOLATION error if the constraint would be violated
- Works in conjunction with deferred constraint checking mechanisms in PostgreSQL

## Simplified Source

```c
Datum RI_FKey_noaction_del(PG_FUNCTION_ARGS) {
    // Validate this is a proper DELETE trigger call
    ri_CheckTrigger(fcinfo, "RI_FKey_noaction_del", RI_TRIGTYPE_DELETE);

    // Use shared constraint logic with NO ACTION behavior (true)
    return ri_restrict((TriggerData *) fcinfo->context, true);
}
```