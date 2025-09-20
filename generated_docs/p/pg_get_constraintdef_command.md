# pg_get_constraintdef_command

## Location
[src/backend/utils/adt/ruleutils.c:2164-2172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L2164-L2172)

## Overview
This internal function returns a complete ALTER TABLE ... ADD CONSTRAINT command for recreating a constraint definition.

## Definition

```c
char *
pg_get_constraintdef_command(Oid constraintId)
```
## Detailed Description
pg_get_constraintdef_command is an internal PostgreSQL function that generates a full SQL command string for recreating a constraint. Unlike the user-facing constraint definition functions that return just the constraint clause, this function returns a complete ALTER TABLE ... ADD CONSTRAINT command that can be executed to recreate the constraint. This is particularly useful during table rebuilding operations where constraints need to be dropped and recreated.

## Parameters / Member Variables
-  (Oid): The object identifier of the constraint for which to generate the recreation command

## Dependencies
- Functions called/Symbols referenced:
  - [pg_get_constraintdef_worker](pg_get_constraintdef_worker.md) (core worker function that generates constraint definition with command flag set to true)
- Called from (representative examples):
  - [RememberConstraintForRebuilding](../R/RememberConstraintForRebuilding.md) (in table command processing)
  - Various internal PostgreSQL rebuild operations

## Notes and Other Information
- This is an internal function not exposed to SQL users
- Returns a complete executable SQL command rather than just a constraint definition
- The function calls pg_get_constraintdef_worker with the  parameter set to true
- Used primarily during table rebuilding operations where constraints need to be preserved
- Located in src/backend/utils/adt/ruleutils.c:2164-2172
- The returned string includes the full ALTER TABLE syntax with table name and ADD CONSTRAINT clause