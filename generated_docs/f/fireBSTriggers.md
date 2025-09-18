# fireBSTriggers

## Location
src/backend/executor/nodeModifyTable.c: 3782 - 3818

## Overview
Executes BEFORE EACH STATEMENT triggers for modify operations (INSERT, UPDATE, DELETE, MERGE) in PostgreSQL's executor.

## Definition


## Detailed Description
The fireBSTriggers function is responsible for firing BEFORE EACH STATEMENT triggers based on the type of modification operation being performed. It analyzes the operation type stored in the ModifyTableState node and calls the appropriate trigger execution function for each operation type. For MERGE operations, it handles multiple subcommands by checking which operations (INSERT, UPDATE, DELETE) are involved in the MERGE and fires the corresponding triggers. For INSERT operations with ON CONFLICT UPDATE clauses, it fires both INSERT and UPDATE triggers.

## Parameters / Member Variables
- : Pointer to ModifyTableState containing the execution state and operation details for the modify table operation

## Dependencies
- Functions called/Symbols referenced:
  - [ExecBSInsertTriggers](../E/ExecBSInsertTriggers.md)
  - [ExecBSUpdateTriggers](../E/ExecBSUpdateTriggers.md)
  - [ExecBSDeleteTriggers](../E/ExecBSDeleteTriggers.md)
  - [ModifyTable](../M/ModifyTable.md) (plan structure)
  - CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE (operation constants)
  - ONCONFLICT_UPDATE (conflict action constant)
  - MERGE_INSERT, MERGE_UPDATE, MERGE_DELETE (merge subcommand flags)
- Called from (representative examples):
  - [ExecModifyTable](../E/ExecModifyTable.md) (at src/backend/executor/nodeModifyTable.c:3995, 3997, 3998)
  - [ExecInitModifyTable](../E/ExecInitModifyTable.md) (at src/backend/executor/nodeModifyTable.c:4489)

## Notes and Other Information
- This function specifically handles BEFORE EACH STATEMENT triggers, which are executed once per SQL statement before any rows are processed
- The function supports all major DML operations including the newer MERGE command
- For MERGE operations, it dynamically determines which triggers to fire based on the subcommands present in the MERGE statement
- INSERT operations with ON CONFLICT UPDATE clauses require both INSERT and UPDATE triggers to be fired
- Located in src/backend/executor/nodeModifyTable.c:3782-3818