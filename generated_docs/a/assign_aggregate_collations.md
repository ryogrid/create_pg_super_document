# assign_aggregate_collations

## Location
src/backend/parser/parse_collate.c: 881 - 918

## Overview
Handles collation assignment for normal aggregate functions, treating ORDER BY expressions separately from regular aggregate arguments to avoid inappropriate collation conflicts.

## Definition


## Detailed Description
This function implements collation assignment logic specific to normal aggregate functions (AGGKIND_NORMAL). It addresses a key challenge: aggregate functions can have both regular arguments that contribute to the result collation and ORDER BY expressions that should not influence the aggregate's collation or conflict with regular arguments.

The function distinguishes between:
1. **Regular aggregate arguments** (): Processed normally through  and contribute to the aggregate's collation
2. **ORDER BY expressions** (): Processed independently through  to avoid collation conflicts

This separation ensures that expressions like  work correctly - the ORDER BY collation doesn't interfere with the aggregate's result collation determined by col1.

## Parameters / Member Variables
- : Pointer to the Aggref node representing the aggregate function call
- : Local collation context for accumulating collation state from regular arguments

## Dependencies
- Functions called/Symbols referenced:
  -  (for ORDER BY expressions that shouldn't affect result collation)
  -  (for regular aggregate arguments)
  -  (list traversal macro)
- Called from (representative examples):
  -  (when processing AGGKIND_NORMAL aggregates)

## Notes and Other Information
- Only applies to normal aggregates - ordered-set and hypothetical aggregates have their own specialized functions
- The function asserts that  is NIL since normal aggregates don't have direct arguments
- [TargetEntry](../T/TargetEntry.md) nodes are processed rather than their contained expressions to ensure proper error reporting for ORDER BY items
- The  and  lists don't need processing since they contain only SortGroupClause nodes without expressions