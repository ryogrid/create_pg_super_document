# mcv_get_match_bitmap

## Location
[src/backend/statistics/mcv.c:1599-2005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L1599-L2005)

## Overview
Evaluates clauses using the MCV (Most Common Values) list and generates a match bitmap indicating which MCV items satisfy the given conditions.

## Definition
```c
static bool *mcv_get_match_bitmap(PlannerInfo *root, List *clauses,
                                 Bitmapset *keys, List *exprs,
                                 MCVList *mcvlist, bool is_or)
```

## Detailed Description
This function is the core evaluation engine for MCV-based selectivity estimation. It processes a list of clauses and determines which MCV items match the specified conditions, maintaining a bitmap to track match/mismatch status for each item. The function handles various clause types including OpExpr (comparison operators), ScalarArrayOpExpr (IN/ANY clauses), NullTest (IS NULL/IS NOT NULL), and boolean expressions (AND/OR/NOT). It supports recursive evaluation of complex boolean expressions and can optimize by skipping items that cannot possibly change the result based on previous evaluations.

## Parameters / Member Variables
- `root`: PlannerInfo containing planner context and statistics
- `clauses`: List of clauses to evaluate against the MCV items
- `keys`: Bitmapset of attribute numbers for simple columns in the statistics
- `exprs`: List of expression nodes for complex expressions in the statistics
- `mcvlist`: The MCV list containing the most common values and their frequencies
- `is_or`: Boolean indicating whether clauses should be combined with OR (true) or AND (false) logic

## Dependencies
- Functions called/Symbols referenced:
  - [MCVList](../M/MCVList.md), STATS_MCVLIST_MAX_ITEMS, is_opclause, OpExpr
  - [get_opcode](../g/get_opcode.md), fmgr_info, examine_opclause_args, mcv_match_expression
  - [MCVItem](../M/MCVItem.md), RESULT_MERGE, RESULT_IS_FINAL, FunctionCall2Coll
  - [ScalarArrayOpExpr](../S/ScalarArrayOpExpr.md), DatumGetArrayTypeP, get_typlenbyvalalign
  - ARR_ELEMTYPE, deconstruct_array, NullTest, is_andclause
  - [is_orclause](../i/is_orclause.md), BoolExpr, is_notclause, bms_member_index
- Called from (representative examples):
  - [mcv_get_match_bitmap](mcv_get_match_bitmap.md) (recursive calls for AND/OR/NOT expressions)
  - [mcv_clauselist_selectivity](mcv_clauselist_selectivity.md)
  - [mcv_clause_selectivity_or](mcv_clause_selectivity_or.md)

## Notes and Other Information
- This is a static function, only accessible within the mcv.c file
- Uses a boolean array bitmap which could be optimized to use single bits (as noted in comments)
- Handles NULL values appropriately by treating them as mismatches for strict operators
- Supports short-circuit evaluation for AND/OR logic to improve performance
- The function is recursive and can handle nested boolean expressions
- Critical component of PostgreSQL's extended statistics system for improved selectivity estimation
- Located in src/backend/statistics/mcv.c:1599-2005