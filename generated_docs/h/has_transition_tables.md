# has_transition_tables

## Location
src/backend/optimizer/util/plancat.c: 2290 - 2343

## Overview
Detects whether a specified relation has any transition tables for a given DML event type, which is used during query planning to determine if transition tables need to be managed for triggers.

## Definition


## Detailed Description
This function checks if a relation referenced by a range table entry has transition tables configured for a specific command type. Transition tables are special tables that store row data during trigger execution, allowing triggers to access OLD and NEW row versions during INSERT, UPDATE, and DELETE operations. The function opens the relation, examines its trigger descriptor, and checks for the presence of transition tables based on the event type.

The function handles different command types:
- INSERT: Checks for new transition tables (trig_insert_new_table)
- UPDATE: Checks for both old and new transition tables (trig_update_old_table, trig_update_new_table)
- DELETE: Checks for old transition tables (trig_delete_old_table)
- MERGE: Always returns false as MERGE uses separate INSERT/UPDATE/DELETE events

Foreign tables are explicitly excluded as they cannot have transition tables.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and information
- : Range table index identifying the relation to check
- : Command type (CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE) to check transition tables for

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - table_open
  - table_close
  - CmdType
  - TriggerDesc
  - RTE_RELATION
  - RELKIND_FOREIGN_TABLE
  - CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE
- Called from (representative examples):
  - make_modifytable

## Notes and Other Information
- The function assumes adequate locking has already been acquired for the relation
- Foreign tables cannot have transition tables and will always return false
- MERGE operations do not have separate transition table handling and return false
- The function is part of the query planner's catalog utilities for determining trigger-related planning requirements