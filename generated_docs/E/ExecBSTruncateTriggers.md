# ExecBSTruncateTriggers

## Location
src/backend/commands/trigger.c: 3307 - 3353

## Overview
Executes BEFORE STATEMENT TRUNCATE triggers that run before a TRUNCATE operation begins, allowing validation or side effects but prohibiting data return.

## Definition


## Detailed Description
This function executes BEFORE STATEMENT TRUNCATE triggers, which fire once per TRUNCATE statement before any rows are actually removed from the table. These triggers operate at the statement level rather than per-row, making them suitable for validation, logging, or other preparatory actions. The function validates that triggers do not attempt to return values (which is prohibited for statement-level triggers) and will raise an error if any trigger violates this protocol.

## Parameters / Member Variables
- : Executor state containing execution context and memory management
- : Relation information including trigger descriptors and table metadata

## Dependencies
- Functions called/Symbols referenced:
  - TriggerEnabled
  - ExecCallTriggerFunc
  - GetPerTupleMemoryContext
  - TRIGGER_TYPE_MATCHES
- Called from (representative examples):
  - ExecuteTruncateGuts

## Notes and Other Information
- Returns immediately if no truncate triggers are defined for the relation
- Only processes triggers matching STATEMENT + BEFORE + TRUNCATE type combination
- Raises an error if any trigger attempts to return a non-NULL tuple value
- Executes triggers synchronously before the actual truncate operation begins
- Does not pass any tuple data since TRUNCATE operates at statement level
- Uses NULL values for old/new slots in TriggerEnabled since no tuples are involved
- Part of the TRUNCATE command execution pipeline in tablecmds.c