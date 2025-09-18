# select_rowmark_type

## Location
src/backend/optimizer/plan/planner.c: 2407 - 2472

## Overview
Determines the appropriate row marking type (RowMarkType) for a given range table entry based on the relation type and locking strength requirements.

## Definition
```c
RowMarkType select_rowmark_type(RangeTblEntry *rte, LockClauseStrength strength)
```

## Detailed Description
This function analyzes a range table entry and the requested lock clause strength to select the most appropriate row marking mechanism. The function handles three main cases:

1. **Non-relation entries**: For any range table entry that is not a relation (such as subqueries, functions, etc.), it defaults to ROW_MARK_COPY, which creates a copy of the row data.

2. **Foreign tables**: For foreign tables, it delegates the decision to the Foreign Data Wrapper (FDW) by calling the FDWs GetForeignRowMarkType function if available. If the FDW doesnt provide this function, it falls back to ROW_MARK_COPY.

3. **Regular tables**: For standard PostgreSQL tables, it maps the lock clause strength to the corresponding row mark type:
   - LCS_NONE → ROW_MARK_REFERENCE (no tuple lock needed)
   - LCS_FORKEYSHARE → ROW_MARK_KEYSHARE
   - LCS_FORSHARE → ROW_MARK_SHARE  
   - LCS_FORNOKEYUPDATE → ROW_MARK_NOKEYEXCLUSIVE
   - LCS_FORUPDATE → ROW_MARK_EXCLUSIVE

## Parameters
- `rte`: Range table entry representing the relation or other data source to be marked
- `strength`: The lock clause strength indicating the desired level of row locking

## Dependencies
- Functions called/Symbols referenced:
  - [GetFdwRoutineByRelId](../G/GetFdwRoutineByRelId.md)
  - Various RowMarkType constants (ROW_MARK_COPY, ROW_MARK_REFERENCE, etc.)
  - LockClauseStrength enum values
  - RTE_RELATION, RELKIND_FOREIGN_TABLE constants
- Called from:
  - [preprocess_rowmarks](../p/preprocess_rowmarks.md)
  - [expand_single_inheritance_child](../e/expand_single_inheritance_child.md)

## Notes and Other Information
- The function includes error handling for unrecognized LockClauseStrength values
- Foreign table row marking behavior is extensible through the FDW interface
- Row marking is essential for implementing SELECT FOR UPDATE/SHARE semantics and ensuring data consistency in concurrent environments
- Located in src/backend/optimizer/plan/planner.c:2407-2472