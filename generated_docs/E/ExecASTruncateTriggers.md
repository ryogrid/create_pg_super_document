# ExecASTruncateTriggers

## Location
src/backend/commands/trigger.c: 3354 - 3370

## Overview
Executes AFTER STATEMENT TRUNCATE triggers for a given table relation during a TRUNCATE operation.

## Definition
```c
void ExecASTruncateTriggers(EState *estate, ResultRelInfo *relinfo)
```

## Detailed Description
This function is responsible for firing AFTER STATEMENT TRUNCATE triggers when a TRUNCATE operation is performed on a table. It checks if the relation has any AFTER STATEMENT TRUNCATE triggers defined and, if so, saves the trigger event for later execution. The function is called as part of the TRUNCATE command execution process to ensure that all appropriate triggers are fired after the truncation has been completed.

The function operates by examining the trigger descriptor of the relation and checking for the presence of after-statement truncate triggers. If such triggers exist, it calls AfterTriggerSaveEvent to queue the trigger for execution.

## Parameters / Member Variables
- `estate`: Execution state information containing context for the current query execution
- `relinfo`: Result relation information structure containing details about the target relation, including its trigger descriptor

## Dependencies
- Functions called/Symbols referenced:
  - AfterTriggerSaveEvent
  - TriggerDesc (struct)
  - TRIGGER_EVENT_TRUNCATE (constant)
- Called from (representative examples):
  - ExecuteTruncateGuts

## Notes and Other Information
- This function only handles AFTER STATEMENT triggers for TRUNCATE operations
- The function does not execute the triggers immediately but saves them for later execution via the after-trigger mechanism
- TRUNCATE triggers are statement-level only (no row-level TRUNCATE triggers exist in PostgreSQL)
- The function is part of PostgreSQL's comprehensive trigger system that ensures data integrity and allows for custom actions during DDL operations