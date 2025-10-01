# has_indexed_join_quals

## Location
[src/backend/optimizer/path/costsize.c:5104-5196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L5104-L5196)

## Overview
Checks whether all the joinquals of a nestloop join are used as inner index quals, determining if an unmatched outer tuple in SEMI/ANTI joins will be cheap to process.

## Definition

```c
static bool
has_indexed_join_quals(NestPath *path)
```
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
  - [join_clause_is_movable_into](../j/join_clause_is_movable_into.md)
  - [is_redundant_with_indexclauses](../i/is_redundant_with_indexclauses.md)
  - [JoinPath](../J/JoinPath.md)
  - [IndexPath](../I/IndexPath.md)  
  - [BitmapHeapPath](../B/BitmapHeapPath.md)
- Called from (representative examples):
  - [final_cost_nestloop](../f/final_cost_nestloop.md)
  - cost_qual_eval_context

## Notes and Other Information
- This is a static function used internally within costsize.c
- Only supports simple index access methods; complex bitmap operations return false
- Requires parameterized inner paths to be meaningful
- Essential for accurate costing of SEMI/ANTI nestloop joins
- The optimization assumes indexed lookups make non-matching outer tuples cheap to skip
- Located in src/backend/optimizer/path/costsize.c:5104-5196

## Simplified Source

```c
static bool has_indexed_join_quals(NestPath *path) {
    JoinPath *joinpath = &path->jpath;
    Path *innerpath = joinpath->innerjoinpath;
    List *indexclauses;
    bool found_one = false;

    // Quick checks: no remaining quals and must be parameterized
    if (joinpath->joinrestrictinfo != NIL || innerpath->param_info == NULL)
        return false;

    // Extract index clauses based on inner path type
    switch (innerpath->pathtype) {
        case T_IndexScan:
        case T_IndexOnlyScan:
            indexclauses = ((IndexPath *) innerpath)->indexclauses;
            break;
        case T_BitmapHeapScan:
            // Only accept simple bitmap scans, not AND/OR combinations
            Path *bmqual = ((BitmapHeapPath *) innerpath)->bitmapqual;
            if (IsA(bmqual, IndexPath))
                indexclauses = ((IndexPath *) bmqual)->indexclauses;
            else
                return false;
            break;
        default:
            return false; // Other path types aren't fast for zero rows
    }

    // Check that all parameter clauses from outer path are covered by index
    foreach(lc, innerpath->param_info->ppi_clauses) {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        if (join_clause_is_movable_into(rinfo, innerpath->parent->relids,
                                        joinpath->path.parent->relids)) {
            if (!is_redundant_with_indexclauses(rinfo, indexclauses))
                return false;
            found_one = true;
        }
    }

    return found_one; // Must have at least one join clause
}
```