# ATPrepChangePersistence

## Location
src/backend/commands/tablecmds.c: 17088 - 17206

## Overview
ATPrepChangePersistence validates and prepares for changing a table's persistence level (LOGGED/UNLOGGED) by checking constraints and publication membership to maintain referential integrity invariants.

## Definition


## Detailed Description
This function serves as the preparation phase for SET LOGGED/UNLOGGED operations in ALTER TABLE commands. It performs several critical validation checks to ensure the persistence change is valid and safe:

1. Prevents persistence changes on temporary tables (which is not allowed)
2. Returns early if the operation is a no-op (table is already in the target persistence state)
3. Validates publication membership constraints (unlogged tables cannot be part of publications)
4. Checks foreign key constraints to preserve the invariant that permanent tables cannot reference unlogged tables

The function examines both incoming and outgoing foreign key relationships and ensures that changing persistence will not violate PostgreSQL's referential integrity rules. Self-referencing foreign keys are safely ignored during this validation.

## Parameters / Member Variables
- : The Relation structure representing the table whose persistence is being changed
- : Boolean indicating whether the change is to LOGGED (true) or UNLOGGED (false)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelationName
  - GetRelationPublications
  - table_open
  - ScanKeyInit
  - systable_beginscan
  - systable_getnext
  - relation_open
  - RelationIsPermanent
  - relation_close
  - systable_endscan
  - table_close

- Called from (representative examples):
  - ATPrepCmd (ALTER TABLE command preparation phase)

## Notes and Other Information
- Returns false if the operation is a no-op, true if the change should proceed
- Uses different scan strategies depending on the direction of the change (conrelid vs confrelid)
- Temporary tables cannot have their persistence changed and will result in an error
- Unlogged tables cannot be part of publications due to replication limitations  
- The function maintains PostgreSQL's invariant that permanent tables cannot reference unlogged tables
- Uses AccessShareLock when examining constraint and foreign table information
- Self-referencing foreign keys are explicitly allowed and ignored during validation
- Publication membership is only checked when changing to UNLOGGED since that's the restriction