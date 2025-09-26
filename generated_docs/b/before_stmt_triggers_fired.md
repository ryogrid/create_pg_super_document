# before_stmt_triggers_fired

## Location
src/backend/commands/trigger.c: 6545 - 6590

## Overview
Detects whether BEFORE STATEMENT triggers have already been queued for a given relation and operation, ensuring triggers fire only once per statement while setting a flag for subsequent calls.

## Definition

```c
static bool
before_stmt_triggers_fired(Oid relid, CmdType cmdType)
```
## Detailed Description
This function implements a crucial mechanism in PostgreSQL's trigger system to ensure that BEFORE STATEMENT triggers fire exactly once per SQL statement, regardless of how many rows are affected or how many times trigger-related functions are called during statement execution.

The function operates using a simple but effective flag-based approach stored in the AfterTriggersTableData structure. On the first call for a specific relation and command type combination, it returns false (indicating triggers haven't been fired yet) and simultaneously sets an internal flag. Subsequent calls for the same relation and operation within the same query context will return true, indicating that triggers have already been queued and should not be queued again.

The state is maintained in the same AfterTriggersTableData structure that holds transition tables for the relation and operation. This design choice is intentional: if the system is forced to create new transition tables because additional tuples are processed after triggers have already fired, it allows a new set of statement triggers to be queued. This ensures proper trigger behavior even in complex scenarios where statement execution involves multiple phases.

The function includes the same safety checks as AfterTriggerSaveEvent, ensuring it's only called within a valid query context and that adequate storage exists for the current query depth.

## Parameters / Member Variables
- : The OID of the relation for which to check trigger firing status
- : The command type (INSERT, UPDATE, DELETE) to check for trigger firing

## Dependencies
- Functions called/Symbols referenced:
  - AfterTriggerEnlargeQueryState (ensures adequate query depth storage)
  - GetAfterTriggersTableData (retrieves or creates table data for the relation/command)
- Called from (representative examples):
  - ExecBSInsertTriggers (before statement INSERT trigger execution)
  - ExecBSDeleteTriggers (before statement DELETE trigger execution)  
  - ExecBSUpdateTriggers (before statement UPDATE trigger execution)

## Notes and Other Information
- The function's design ensures BEFORE STATEMENT triggers fire exactly once per statement, which is a fundamental requirement of the SQL standard
- The state is tied to transition table data, allowing proper behavior when multiple transition table sets are created during complex statement execution
- The flag-setting behavior means the first call returns false but sets up subsequent calls to return true
- This mechanism works in conjunction with similar logic for AFTER STATEMENT triggers to maintain proper trigger firing semantics
- The function includes the same query depth validation as other trigger-related functions to prevent misuse outside of valid execution contexts