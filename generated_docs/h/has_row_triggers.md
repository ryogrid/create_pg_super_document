# has_row_triggers

## Location
[src/backend/optimizer/util/plancat.c:2240-2289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L2240-L2289)

## Overview
Detects whether a specified relation has any row-level triggers for a given DML event (INSERT, UPDATE, DELETE).

## Definition
```c
bool has_row_triggers(PlannerInfo *root, Index rti, CmdType event)
```

## Detailed Description
The has_row_triggers function is used by PostgreSQL's query planner to determine whether a relation has row-level triggers defined for a specific DML operation. This information is crucial for planning decisions, as the presence of row-level triggers can significantly impact the execution strategy and cost estimates for data modification operations.

The function operates by:

1. **Relation Access**: It retrieves the RangeTblEntry for the specified relation using planner_rt_fetch() and opens the relation using table_open() with NoLock (assuming adequate locking already exists).

2. **Trigger Inspection**: It examines the relation's TriggerDesc structure, which contains metadata about all triggers defined on the relation.

3. **Event-Specific Checking**: Based on the provided CmdType, it checks for the presence of both BEFORE and AFTER row-level triggers:
   - **CMD_INSERT**: Checks trig_insert_before_row and trig_insert_after_row flags
   - **CMD_UPDATE**: Checks trig_update_before_row and trig_update_after_row flags  
   - **CMD_DELETE**: Checks trig_delete_before_row and trig_delete_after_row flags
   - **CMD_MERGE**: Always returns false, as MERGE operations are handled through their constituent INSERT/UPDATE/DELETE components

The function properly manages relation access by closing the relation after inspection, maintaining the same lock level that was held on entry.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and range table information
- `rti`: Index into the range table identifying the relation to check
- `event`: CmdType specifying the DML operation (CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE)

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [table_open](../t/table_open.md)
  - [table_close](../t/table_close.md)
  - [TriggerDesc](../T/TriggerDesc.md)
  - CmdType
  - CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_MERGE
- Called from (representative examples):
  - [make_modifytable](../m/make_modifytable.md)

## Notes and Other Information
The function assumes that the caller already holds adequate locking on the relation, so it uses NoLock when opening and closing the relation. This is a common pattern in planner code where relations are accessed for metadata inspection. The presence of row-level triggers affects planning decisions because triggers can modify data, perform additional operations, or even prevent the operation from completing, all of which impact cost estimates and execution strategies. The function explicitly handles the MERGE command by returning false, as MERGE trigger handling is managed through the individual INSERT/UPDATE/DELETE operations that comprise the MERGE.