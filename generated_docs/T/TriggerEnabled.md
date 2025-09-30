# TriggerEnabled

## Location
[src/backend/commands/trigger.c:3509-3631](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3509-L3631)

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
  - [bms_is_member](../b/bms_is_member.md)
  - [stringToNode](../s/stringToNode.md)
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
  - [make_ands_implicit](../m/make_ands_implicit.md)
  - [ExecPrepareQual](../E/ExecPrepareQual.md)
  - GetPerTupleExprContext
  - [ExecQual](../E/ExecQual.md)
  - TRIGGER_FIRED_BY_UPDATE
  - SESSION_REPLICATION_ROLE_REPLICA
  - TRIGGER_FIRES_ON_ORIGIN/TRIGGER_FIRES_ON_REPLICA
  - TRIGGER_DISABLED
- Called from (representative examples):
  - [ExecBSInsertTriggers](../E/ExecBSInsertTriggers.md)
  - [ExecBRInsertTriggers](../E/ExecBRInsertTriggers.md)
  - [ExecBSDeleteTriggers](../E/ExecBSDeleteTriggers.md)
  - [ExecBRDeleteTriggersNew](../E/ExecBRDeleteTriggersNew.md)
  - [ExecBSUpdateTriggers](../E/ExecBSUpdateTriggers.md)
  - [ExecBRUpdateTriggersNew](../E/ExecBRUpdateTriggersNew.md)
  - [AfterTriggerSaveEvent](../A/AfterTriggerSaveEvent.md)

## Notes and Other Information
- The function is static and used internally within the trigger execution system
- WHEN expressions are compiled lazily and cached in the per-query memory context
- Column-specific triggers only apply to UPDATE events and are ignored for other trigger types
- The function properly handles variable references in WHEN clauses by converting OLD/NEW references to INNER_VAR/OUTER_VAR
- Replication role checks ensure triggers fire appropriately in master-slave replication scenarios
- Returns true if the trigger should fire, false if it should be skipped

## Simplified Source

```c
static bool
TriggerEnabled(EState *estate, ResultRelInfo *relinfo,
               Trigger *trigger, TriggerEvent event,
               Bitmapset *modifiedCols,
               TupleTableSlot *oldslot, TupleTableSlot *newslot)
{
    // Check replication-role-dependent enable state
    if (SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA) {
        if (trigger->tgenabled == TRIGGER_FIRES_ON_ORIGIN ||
            trigger->tgenabled == TRIGGER_DISABLED)
            return false;
    } else {
        // ORIGIN or LOCAL role
        if (trigger->tgenabled == TRIGGER_FIRES_ON_REPLICA ||
            trigger->tgenabled == TRIGGER_DISABLED)
            return false;
    }

    // Check for column-specific trigger (UPDATE only)
    if (trigger->tgnattr > 0 && TRIGGER_FIRED_BY_UPDATE(event)) {
        bool modified = false;
        for (int i = 0; i < trigger->tgnattr; i++) {
            if (bms_is_member(trigger->tgattr[i] - FirstLowInvalidHeapAttributeNumber,
                             modifiedCols)) {
                modified = true;
                break;
            }
        }
        if (!modified)
            return false;
    }

    // Check for WHEN clause
    if (trigger->tgqual) {
        ExprState **predicate;
        ExprContext *econtext;
        int i;

        // Find matching element in ri_TrigWhenExprs
        i = trigger - relinfo->ri_TrigDesc->triggers;
        predicate = &relinfo->ri_TrigWhenExprs[i];

        // Compile WHEN expression if first time
        if (*predicate == NULL) {
            MemoryContext oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
            Node *tgqual = stringToNode(trigger->tgqual);

            // Convert OLD/NEW references to INNER_VAR/OUTER_VAR
            ChangeVarNodes(tgqual, PRS2_OLD_VARNO, INNER_VAR, 0);
            ChangeVarNodes(tgqual, PRS2_NEW_VARNO, OUTER_VAR, 0);
            tgqual = (Node *) make_ands_implicit((Expr *) tgqual);

            *predicate = ExecPrepareQual((List *) tgqual, estate);
            MemoryContextSwitchTo(oldContext);
        }

        // Evaluate WHEN expression
        econtext = GetPerTupleExprContext(estate);
        econtext->ecxt_innertuple = oldslot;
        econtext->ecxt_outertuple = newslot;

        if (!ExecQual(*predicate, econtext))
            return false;
    }

    return true;
}
```