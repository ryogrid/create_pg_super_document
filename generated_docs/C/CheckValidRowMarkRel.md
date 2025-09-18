# CheckValidRowMarkRel

## Location
src/backend/executor/execMain.c: 1131 - 1195

## Overview
Validates that a proposed rowmark target relation is a legal target for row locking operations, checking relation types and FDW capabilities to ensure proper access control.

## Definition


## Detailed Description
CheckValidRowMarkRel performs runtime validation of row marking (locking) operations on different types of relations. While the parser and planner catch most invalid cases, this function provides a final safety check during execution. It examines the relation's kind and determines whether the specified row mark type is permitted, with special handling for foreign tables that require FDW support for row refetching.

## Parameters / Member Variables
- : The target relation to validate for row marking operations
- : The type of row marking being attempted (e.g., ROW_MARK_REFERENCE, explicit locking clauses)

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