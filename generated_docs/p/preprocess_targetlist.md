# preprocess_targetlist

## Location
[src/backend/optimizer/prep/preptlist.c:64-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/preptlist.c#L64-L347)

## Overview
Driver function for preprocessing the parse tree targetlist, handling different command types (INSERT, UPDATE, DELETE, MERGE, SELECT) and preparing the targetlist for query execution.

## Definition


## Detailed Description
The  function is the main entry point for targetlist preprocessing in PostgreSQL's query planner. It takes a parsed query and transforms its targetlist to prepare it for execution, handling the specific requirements of different SQL command types.

For INSERT commands, it expands the targetlist to match the exact order of the target table's attributes using . For UPDATE commands, it extracts column numbers being updated via  and renumbers the processed targetlist entries to be consecutive.

The function also handles special cases like MERGE commands (which process each action's targetlist separately), adds row identity columns for UPDATE/DELETE/MERGE operations, manages junk columns for row locking (FOR UPDATE/SHARE), and processes RETURNING clauses.

The preprocessed targetlist is stored in , and for UPDATE operations, the target column numbers are stored in .

## Parameters / Member Variables
- : PlannerInfo structure containing the parsed query and planning state information

## Dependencies
- Functions called/Symbols referenced:
  - [expand_insert_targetlist](../e/expand_insert_targetlist.md)
  - [extract_update_targetlist_colnos](../e/extract_update_targetlist_colnos.md)
  - [add_row_identity_columns](../a/add_row_identity_columns.md)
  - rt_fetch
  - table_open/table_close
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - makeVar
  - makeWholeRowVar
  - [pull_var_clause](pull_var_clause.md)
  - [tlist_member](../t/tlist_member.md)
  - [list_concat_copy](../l/list_concat_copy.md)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md) (src/backend/optimizer/plan/planner.c:1470)

## Notes and Other Information
This function is located in src/backend/optimizer/prep/preptlist.c:64-347 and serves as a critical component in PostgreSQL's query planning phase. It must handle the complexities of different SQL command types while ensuring the targetlist is properly formatted for the executor. The function carefully manages memory and maintains proper reference relationships between query elements.