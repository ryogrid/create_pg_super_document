# get_relation_statistics

## Location
src/backend/optimizer/util/plancat.c: 1470 - 1575

## Overview
Retrieves extended statistics metadata defined on a table and prepares StatisticExtInfo objects containing identifying information for the optimizer's use.

## Definition


## Detailed Description
The  function discovers and processes extended statistics objects defined on a relation, creating  structures that contain metadata needed by the query optimizer. The function does not load the actual statistics data but focuses on preparing the identifying information and expression processing.

For each statistics object found, the function builds a bitmapset of covered columns and processes any expressions defined in the statistics object. Expression processing includes constant evaluation and varno adjustment to ensure compatibility with query planning operations. The function calls  twice for each statistics object - once for inherited statistics and once for non-inherited statistics.

The function handles both column-based and expression-based extended statistics, ensuring that expressions are properly normalized through  and have their variable references updated to match the relation's varno in the current query context.

## Parameters / Member Variables
- : RelOptInfo structure representing the relation in the optimizer
- : The actual relation object to extract statistics information from

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetStatExtList
  - Form_pg_statistic_ext
  - bms_add_member
  - SysCacheGetAttr
  - TextDatumGetCString
  - stringToNode
  - eval_const_expressions
  - fix_opfuncids
  - ChangeVarNodes
  - get_relation_statistics_worker
  - bms_free
  - list_free
- Called from (representative examples):
  - get_relation_info

## Notes and Other Information
- This is a static function, not part of the external API
- Only processes statistics objects that have actually been built (verified by the worker function)
- Processes expressions through the same pipeline as qual clauses for proper matching
- Creates separate StatisticExtInfo entries for inherited and non-inherited statistics
- Properly manages memory by freeing temporary data structures
- Uses 1-based column indexing consistent with PostgreSQL conventions
- The function assumes statistics objects exist but may not have actual data built
- Expression processing includes opfuncid fixing for optimization purposes