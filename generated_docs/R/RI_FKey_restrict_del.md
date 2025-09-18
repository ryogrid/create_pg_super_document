# RI_FKey_restrict_del

## Location
src/backend/utils/adt/ri_triggers.c: 571 - 587

## Overview
Trigger function that enforces RESTRICT foreign key constraint behavior during DELETE operations on the primary key table, immediately preventing deletions that would violate referential integrity.

## Definition


## Detailed Description
This function implements the RESTRICT foreign key constraint action for DELETE operations on primary key tables. When a row in a primary key table is being deleted, this function immediately checks whether any foreign key rows in referencing tables depend on the key being deleted. If dependencies exist, it immediately prevents the deletion by raising a foreign key violation error.

The key difference between RESTRICT and NO ACTION constraints is timing and behavior:
- RESTRICT checks are performed immediately when the delete is attempted, with no possibility for deferral
- RESTRICT does not allow another primary key row with the same values to "substitute" for the deleted row
- The constraint violation is detected and reported as soon as the DELETE operation is processed

According to the SQL standard, RESTRICT should occur exactly when the delete is performed, rather than after. PostgreSQL implements this as a non-deferrable AFTER trigger to maintain consistency with its trigger architecture while preserving the immediate enforcement semantics.

The function delegates the main constraint checking logic to the  function, passing  for the  parameter to ensure strict RESTRICT behavior.

## Parameters / Member Variables
This function follows PostgreSQL's trigger function interface:
- Uses  macro which provides access to 
-  contains the  structure with information about the DELETE operation being performed

## Dependencies
- Functions called/Symbols referenced:
  - [ri_CheckTrigger](../r/ri_CheckTrigger.md)
  - [ri_restrict](../r/ri_restrict.md)
  - RI_TRIGTYPE_DELETE (constant)
  - TriggerData (type cast)
- Called from (representative examples):
  - No direct references found in the codebase analysis

## Notes and Other Information
- This function is typically installed as a trigger function on primary key tables for DELETE events
- Implemented as a non-deferrable AFTER trigger to ensure immediate constraint enforcement
- The main constraint validation logic is shared with NO ACTION through the  function
- RESTRICT behavior is stricter than NO ACTION - no substitution of primary key rows is allowed
- Located in src/backend/utils/adt/ri_triggers.c:571-587
- Part of PostgreSQL's comprehensive foreign key constraint system
- Raises FOREIGN_KEY_VIOLATION error immediately if the constraint would be violated
- Follows SQL standard semantics for RESTRICT referential actions
- The non-deferrable nature ensures that constraint violations are caught as early as possible in the transaction