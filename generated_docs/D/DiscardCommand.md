# DiscardCommand

## Location
src/backend/commands/discard.c: 31 - 56

## Overview
Implements the SQL DISCARD statement functionality, handling different discard targets including ALL, PLANS, SEQUENCES, and TEMP objects.

## Definition
```c
void DiscardCommand(DiscardStmt *stmt, bool isTopLevel)
```

## Detailed Description
DiscardCommand is the main entry point for executing SQL DISCARD statements in PostgreSQL. It takes a parsed DISCARD statement and routes the execution to the appropriate cleanup function based on the specified target. The function supports four discard targets:
- DISCARD ALL: Discards all session state by calling DiscardAll()
- DISCARD PLANS: Resets the plan cache using ResetPlanCache()
- DISCARD SEQUENCES: Resets sequence caches via ResetSequenceCaches()
- DISCARD TEMP: Resets temporary table namespace through ResetTempTableNamespace()

The function is designed to handle different levels of session cleanup, allowing fine-grained control over what state to discard or comprehensive cleanup with DISCARD ALL.

## Parameters / Member Variables
- `stmt`: Pointer to DiscardStmt structure containing the parsed DISCARD statement with the target type
- `isTopLevel`: Boolean indicating whether this command is executed at the top level (used for transaction safety checks)

## Dependencies
- Functions called/Symbols referenced:
  - [DiscardAll](DiscardAll.md)
  - [ResetPlanCache](../R/ResetPlanCache.md)
  - [ResetSequenceCaches](../R/ResetSequenceCaches.md)
  - [ResetTempTableNamespace](../R/ResetTempTableNamespace.md)
  - DiscardStmt (structure)
  - DISCARD_ALL, DISCARD_PLANS, DISCARD_SEQUENCES, DISCARD_TEMP (enum values)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- The function uses a switch statement to handle different discard targets efficiently
- Error handling is provided for unrecognized discard targets with elog(ERROR)
- The isTopLevel parameter is passed through to DiscardAll() for transaction safety validation
- Located in src/backend/commands/discard.c, which is dedicated to DISCARD command implementation
- Part of PostgreSQL's session state management system