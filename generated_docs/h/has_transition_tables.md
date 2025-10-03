# has_transition_tables

## Location
[src/backend/optimizer/util/plancat.c:2290-2343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L2290-L2343)

## Overview
Detects whether a specified relation has any transition tables for a given DML event type, which is used during query planning to determine if transition tables need to be managed for triggers.

## Definition

```c
bool
has_transition_tables(PlannerInfo *root, Index rti, CmdType event)
```
## Detailed Description
This function checks if a relation referenced by a range table entry has transition tables configured for a specific command type. Transition tables are special tables that store row data during trigger execution, allowing triggers to access OLD and NEW row versions during INSERT, UPDATE, and DELETE operations. The function opens the relation, examines its trigger descriptor, and checks for the presence of transition tables based on the event type.

The function handles different command types:
- INSERT: Checks for new transition tables (trig_insert_new_table)
- UPDATE: Checks for both old and new transition tables (trig_update_old_table, trig_update_new_table)
- DELETE: Checks for old transition tables (trig_delete_old_table)
- MERGE: Always returns false as MERGE uses separate INSERT/UPDATE/DELETE events

Foreign tables are explicitly excluded as they cannot have transition tables.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planning context and information
- `rti`: Range table index identifying the relation to check
- `event`: Command type (CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE) to check transition tables for
## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [table_open](../t/table_open.md)
  - [table_close](../t/table_close.md)
  - CmdType
  - [TriggerDesc](../T/TriggerDesc.md)
  - RTE_RELATION
  - RELKIND_FOREIGN_TABLE
  - CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE
- Called from (representative examples):
  - [make_modifytable](../m/make_modifytable.md)

## Notes and Other Information
- The function assumes adequate locking has already been acquired for the relation
- Foreign tables cannot have transition tables and will always return false
- MERGE operations do not have separate transition table handling and return false
- The function is part of the query planner's catalog utilities for determining trigger-related planning requirements

## Simplified Source

```c
bool
has_transition_tables(PlannerInfo *root, Index rti, CmdType event)
{
    // Get the range table entry
    RangeTblEntry *rte = planner_rt_fetch(rti, root);
    bool result = false;

    Assert(rte->rtekind == RTE_RELATION);

    // Foreign tables cannot have transition tables
    if (rte->relkind == RELKIND_FOREIGN_TABLE)
        return false;

    // Open the relation and get trigger descriptor
    Relation relation = table_open(rte->relid, NoLock);
    TriggerDesc *trigDesc = relation->trigdesc;

    // Check for transition tables based on command type
    switch (event)
    {
        case CMD_INSERT:
            if (trigDesc && trigDesc->trig_insert_new_table)
                result = true;
            break;

        case CMD_UPDATE:
            if (trigDesc &&
                (trigDesc->trig_update_old_table ||
                 trigDesc->trig_update_new_table))
                result = true;
            break;

        case CMD_DELETE:
            if (trigDesc && trigDesc->trig_delete_old_table)
                result = true;
            break;

        case CMD_MERGE:
            // MERGE uses separate INSERT/UPDATE/DELETE events
            result = false;
            break;

        default:
            elog(ERROR, "unrecognized CmdType: %d", (int) event);
            break;
    }

    // Clean up and return result
    table_close(relation, NoLock);
    return result;
}
```