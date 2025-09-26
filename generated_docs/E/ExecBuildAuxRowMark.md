# ExecBuildAuxRowMark

## Location
[src/backend/executor/execMain.c:2402-2471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L2402-L2471)

## Overview
Creates an ExecAuxRowMark structure that maps row mark information to the specific junk columns in the target list, enabling efficient access to row identification data during execution.

## Definition
ExecAuxRowMark *ExecBuildAuxRowMark(ExecRowMark *erm, List *targetlist)

## Detailed Description
ExecBuildAuxRowMark constructs an auxiliary row mark structure that connects an ExecRowMark with the actual column positions of the junk columns needed for row identification and locking. The function determines which junk columns are required based on the row mark type:
- For non-COPY row marks: requires the 'ctid' column for tuple identification
- For COPY row marks: requires the 'wholerow' column containing the entire row data
- For child relations (inheritance): additionally requires the 'tableoid' column to identify which table the row belongs to

The function searches the target list for these specially named junk columns and stores their attribute numbers in the ExecAuxRowMark structure for efficient runtime access.

## Parameters / Member Variables
- `erm`: ExecRowMark structure containing the row marking configuration and metadata
- `targetlist`: Target list of the input plan node containing the junk columns with row identification information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecRowMark](ExecRowMark.md) (structure type)
  - [ExecAuxRowMark](ExecAuxRowMark.md) (structure type)
  - ROW_MARK_COPY (constant)
  - [ExecFindJunkAttributeInTlist](ExecFindJunkAttributeInTlist.md)
  - AttributeNumberIsValid
  - [palloc0](../p/palloc0.md)
  - snprintf
  - elog
- Called from (representative examples):
  - [ExecInitLockRows](ExecInitLockRows.md)
  - [ExecInitModifyTable](ExecInitModifyTable.md)

## Notes and Other Information
This function is part of PostgreSQL's row locking infrastructure setup phase. The junk columns it searches for are added by the planner and have standardized naming conventions: 'ctid[N]', 'wholerow[N]', and 'tableoid[N]' where N is the rowmarkId. The function performs error checking to ensure all required junk columns are present, as their absence would indicate a planner bug or corrupted plan tree. The distinction between rti and prti helps identify child relations in inheritance hierarchies where tableoid is needed to determine the specific table.