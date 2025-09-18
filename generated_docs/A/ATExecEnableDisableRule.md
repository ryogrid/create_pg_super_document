# ATExecEnableDisableRule

## Location
[src/backend/commands/tablecmds.c:15622-15638](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L15622-L15638)

## Overview
Executes ALTER TABLE ENABLE/DISABLE RULE commands by delegating to the rewrite rule subsystem and invoking post-alter hooks.

## Definition


## Detailed Description
The  function is the execution handler for ALTER TABLE ENABLE/DISABLE RULE commands within the ALTER TABLE infrastructure. It serves as a wrapper that delegates the actual rule manipulation to the rewrite rule subsystem while ensuring proper integration with the ALTER TABLE framework, including invoking necessary post-alter hooks for event triggers and dependency tracking.

## Parameters / Member Variables
- : The relation (table/view) containing the rule to be enabled or disabled
- : The name of the rule to enable or disable
- : Character indicating when the rule should fire (e.g., 'O' for ORIGIN, 'D' for DISABLED, 'R' for REPLICA, 'A' for ALWAYS)
- : The lock mode to use during the operation (parameter is accepted but not directly used in the implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [EnableDisableRule](../E/EnableDisableRule.md)
  - InvokeObjectPostAlterHook
  - RelationRelationId
  - RelationGetRelid
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (multiple rule-related ALTER TABLE subcases)

## Notes and Other Information
- Part of the ALTER TABLE command execution infrastructure  
- Supports different rule firing modes: ORIGIN, DISABLED, REPLICA, ALWAYS
- Handles rewrite rules on both tables and views
- Integrates with the event trigger system through post-alter hooks
- The actual rule manipulation logic is implemented in the rewrite rule subsystem (rewriteDefine.c)
- Unlike the trigger version, this function does not support recursion or skip_system flags
- Rules are part of PostgreSQL's query rewrite system for implementing views and other query transformations