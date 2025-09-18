# ExecBSInsertTriggers

## Location
src/backend/commands/trigger.c: 2396 - 2446

## Overview
Executes all enabled BEFORE STATEMENT INSERT triggers for a given relation, ensuring they fire only once per statement and enforcing trigger protocol rules.

## Definition
```c
void ExecBSInsertTriggers(EState *estate, ResultRelInfo *relinfo)
```

## Detailed Description
This function manages the execution of BEFORE STATEMENT INSERT triggers, which fire once per INSERT statement before any rows are processed. It implements several important safeguards and optimizations:

1. **Duplicate Prevention**: Uses before_stmt_triggers_fired() to ensure triggers don't fire multiple times for the same statement context
2. **Trigger Filtering**: Only executes triggers that match the exact criteria (BEFORE + STATEMENT + INSERT)
3. **Enable Checking**: Respects trigger enable/disable states and trigger conditions
4. **Protocol Enforcement**: Validates that BEFORE STATEMENT triggers don't return values (which would violate the trigger protocol)

The function iterates through all triggers defined on the relation, filtering for the appropriate type, checking if they're enabled, and executing them via ExecCallTriggerFunc. Any attempt by a BEFORE STATEMENT trigger to return a tuple results in an error.

## Parameters / Member Variables
- `estate`: Executor state containing execution context and memory management information
- `relinfo`: Result relation info containing trigger descriptors, function cache, and relation metadata

## Dependencies
- Functions called/Symbols referenced:
  - before_stmt_triggers_fired (duplicate execution prevention)
  - TRIGGER_TYPE_MATCHES (trigger type filtering macro)
  - TriggerEnabled (trigger enable state checking)
  - ExecCallTriggerFunc (actual trigger execution)
  - GetPerTupleMemoryContext (memory context management)
- Data structures used:
  - TriggerDesc (trigger descriptor from relinfo)
  - TriggerData (trigger execution context)
  - Trigger (individual trigger structure)
- Called from (representative examples):
  - CopyFrom (during COPY FROM operations)
  - fireBSTriggers (from nodeModifyTable executor)

## Notes and Other Information
- BEFORE STATEMENT triggers fire exactly once per SQL statement, regardless of how many rows are affected
- These triggers cannot access individual row data since they execute before any rows are processed
- The function enforces the trigger protocol by rejecting any non-NULL return values
- Trigger execution uses per-tuple memory context for proper cleanup
- The function short-circuits if no triggers exist or if no INSERT BEFORE STATEMENT triggers are defined
- Used in both regular INSERT operations and bulk operations like COPY FROM
- Part of PostgreSQL's comprehensive trigger system that supports multiple timing and granularity combinations