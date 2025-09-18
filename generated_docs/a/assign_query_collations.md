# assign_query_collations

## Location
src/backend/parser/parse_collate.c: 101 - 125

## Overview
Marks all expressions in a given Query with collation information after completion of parse analysis.

## Definition


## Detailed Description
This function serves as the main entry point for assigning collation information to all expressions within a parsed Query structure. It utilizes query_tree_walker() to traverse the query tree and apply collation assignment to contained expressions. The function specifically avoids recursing into sub-Queries since those should have been processed when they were built. It also skips the range table and CTE subqueries, as RTEs and subqueries must have been processed already to ensure that Vars referring to them are created with the correct collation.

## Parameters / Member Variables
- : ParseState context containing parsing state information
- : The Query structure whose expressions need collation assignment

## Dependencies
- Functions called/Symbols referenced:
  - query_tree_walker
  - assign_query_collations_walker
  - QTW_IGNORE_RANGE_TABLE
  - QTW_IGNORE_CTE_SUBQUERIES
- Called from (representative examples):
  - transformDeleteStmt (src/backend/parser/analyze.c:566)
  - transformInsertStmt (src/backend/parser/analyze.c:992)
  - transformSelectStmt (src/backend/parser/analyze.c:1463)
  - transformValuesClause (src/backend/parser/analyze.c:1683)
  - transformSetOperationStmt (src/backend/parser/analyze.c:1940)
  - transformReturnStmt (src/backend/parser/analyze.c:2408)
  - transformUpdateStmt (src/backend/parser/analyze.c:2475)
  - transformMergeStmt (src/backend/parser/parse_merge.c:409)

## Notes and Other Information
This function should be applied to each Query after completion of parse analysis for expressions. It deliberately ignores range tables and CTE subqueries during traversal, assuming they have been properly processed during their creation phase. The function is defined in src/backend/parser/parse_collate.c at lines 101-125.