# has_indexed_join_quals

## Location
src/backend/optimizer/path/costsize.c: 5104 - 5196

## Overview
Checks whether all the joinquals of a nestloop join are used as inner index quals, determining if an unmatched outer tuple in SEMI/ANTI joins will be cheap to process.

## Definition


## Detailed Description
This function determines whether a nestloop join path can process unmatched outer tuples efficiently by checking if all join qualifications are handled as index qualifications on the inner path. This optimization is particularly important for SEMI/ANTI joins where:

- If all joinquals are indexed, unmatched outer tuples are cheap to process
- If joinquals are not fully indexed, unmatched outer tuples are expensive to process

The function performs several checks:
1. Verifies no additional quals remain to be evaluated at the join level
2. Ensures the inner path is parameterized (otherwise no optimization applies)  
3. Identifies indexclauses from IndexScan, IndexOnlyScan, or simple BitmapHeapScan paths
4. Validates that all parameter clauses from the outer path are covered by index clauses
5. Requires at least one join clause to avoid clauseless joins

For BitmapHeapScan paths, only simple bitmap scans are accepted, not complex AND/OR combinations.

## Parameters / Member Variables
- : NestPath representing the nested loop join path to analyze

## Dependencies
- Functions called/Symbols referenced:
  - join_clause_is_movable_into
  - is_redundant_with_indexclauses
  - JoinPath
  - IndexPath  
  - BitmapHeapPath
- Called from (representative examples):
  - final_cost_nestloop
  - cost_qual_eval_context

## Notes and Other Information
- This is a static function used internally within costsize.c
- Only supports simple index access methods; complex bitmap operations return false
- Requires parameterized inner paths to be meaningful
- Essential for accurate costing of SEMI/ANTI nestloop joins
- The optimization assumes indexed lookups make non-matching outer tuples cheap to skip
- Located in src/backend/optimizer/path/costsize.c:5104-5196