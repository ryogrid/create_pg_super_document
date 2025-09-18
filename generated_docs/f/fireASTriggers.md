# fireASTriggers

## Location
src/backend/executor/nodeModifyTable.c: 3819 - 3863

## Overview
Executes AFTER EACH STATEMENT triggers for modify operations (INSERT, UPDATE, DELETE, MERGE) in PostgreSQL's executor, handling transition table capture for statement-level triggers.

## Definition
```c
static void fireASTriggers(ModifyTableState *node)
```

## Detailed Description
The fireASTriggers function is responsible for firing AFTER EACH STATEMENT triggers based on the type of modification operation being performed. Unlike BEFORE triggers, AFTER triggers have access to transition tables that capture the rows affected by the statement. The function analyzes the operation type and calls the appropriate AFTER trigger execution function, passing the relevant transition capture state. For MERGE operations, it processes subcommands in a specific order (DELETE, UPDATE, INSERT) to ensure proper trigger execution semantics. For INSERT operations with ON CONFLICT UPDATE clauses, it fires UPDATE triggers first (with ON CONFLICT transition capture) followed by INSERT triggers.

## Parameters / Member Variables
- `node`: Pointer to ModifyTableState containing the execution state, operation details, and transition capture information for the modify table operation

## Dependencies
- Functions called/Symbols referenced:
  - ExecASInsertTriggers
  - ExecASUpdateTriggers
  - ExecASDeleteTriggers
  - ModifyTable (plan structure)
  - CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE (operation constants)
  - ONCONFLICT_UPDATE (conflict action constant)
  - MERGE_INSERT, MERGE_UPDATE, MERGE_DELETE (merge subcommand flags)
- Called from (representative examples):
  - ExecModifyTable (at src/backend/executor/nodeModifyTable.c:4355)

## Notes and Other Information
- This function specifically handles AFTER EACH STATEMENT triggers, which are executed once per SQL statement after all rows have been processed
- AFTER triggers receive transition table capture information (mt_transition_capture, mt_oc_transition_capture) to access OLD/NEW transition tables
- For MERGE operations, triggers are fired in reverse order compared to BEFORE triggers (DELETE, UPDATE, INSERT)
- INSERT operations with ON CONFLICT UPDATE use separate transition capture states for the conflict resolution (mt_oc_transition_capture) and the main operation (mt_transition_capture)
- The function ensures that transition tables contain the appropriate row data for each trigger type
- Located in src/backend/executor/nodeModifyTable.c:3819-3863