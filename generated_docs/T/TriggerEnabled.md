# TriggerEnabled

## Location
src/backend/commands/trigger.c: 3509 - 3631

## Overview
Determines whether a trigger should fire based on replication role, column modifications, and WHEN clause conditions.

## Definition
```c
static bool TriggerEnabled(EState *estate, ResultRelInfo *relinfo,
                          Trigger *trigger, TriggerEvent event,
                          Bitmapset *modifiedCols,
                          TupleTableSlot *oldslot, TupleTableSlot *newslot)
```

## Detailed Description
This function implements the comprehensive logic for determining whether a specific trigger should be executed. It performs multiple levels of checks to ensure triggers fire only when appropriate:

1. **Replication Role Check**: Verifies if the trigger is enabled for the current session replication role (ORIGIN, REPLICA, or LOCAL)
2. **Column-Specific Triggers**: For UPDATE triggers, checks if any of the trigger's specified columns were actually modified
3. **WHEN Clause Evaluation**: If the trigger has a WHEN clause, evaluates the conditional expression using the old and new tuple values

The function handles the lazy compilation of WHEN expressions, converting stored string representations into executable expression trees and caching them for subsequent use. It uses PostgreSQL's expression evaluation framework to assess WHEN conditions with proper tuple context.

## Parameters / Member Variables
- `estate`: Execution state containing query context and expression evaluation infrastructure
- `relinfo`: Result relation information containing trigger descriptors and compiled expressions
- `trigger`: The specific trigger being evaluated for execution
- `event`: The trigger event type (INSERT, UPDATE, DELETE, TRUNCATE)
- `modifiedCols`: Bitmapset indicating which columns were modified (relevant for UPDATE triggers)
- `oldslot`: Tuple slot containing the old tuple values (for UPDATE/DELETE triggers)
- `newslot`: Tuple slot containing the new tuple values (for INSERT/UPDATE triggers)

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_member
  - stringToNode
  - ChangeVarNodes
  - make_ands_implicit
  - ExecPrepareQual
  - GetPerTupleExprContext
  - ExecQual
  - TRIGGER_FIRED_BY_UPDATE
  - SESSION_REPLICATION_ROLE_REPLICA
  - TRIGGER_FIRES_ON_ORIGIN/TRIGGER_FIRES_ON_REPLICA
  - TRIGGER_DISABLED
- Called from (representative examples):
  - ExecBSInsertTriggers
  - ExecBRInsertTriggers
  - ExecBSDeleteTriggers
  - ExecBRDeleteTriggersNew
  - ExecBSUpdateTriggers
  - ExecBRUpdateTriggersNew
  - AfterTriggerSaveEvent

## Notes and Other Information
- The function is static and used internally within the trigger execution system
- WHEN expressions are compiled lazily and cached in the per-query memory context
- Column-specific triggers only apply to UPDATE events and are ignored for other trigger types
- The function properly handles variable references in WHEN clauses by converting OLD/NEW references to INNER_VAR/OUTER_VAR
- Replication role checks ensure triggers fire appropriately in master-slave replication scenarios
- Returns true if the trigger should fire, false if it should be skipped