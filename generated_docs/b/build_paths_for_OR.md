# build_paths_for_OR

## Location
[src/backend/optimizer/path/indxpath.c:1086-1179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L1086-L1179)

## Overview
 constructs all matching IndexPaths for a relation given restriction clauses from one arm of an OR clause, scanning all indexes to support bitmap OR trees.

## Definition

```c
struct paths if possible.
		 */
		indexpaths = build_index_paths(root, rel,
									   index, &clauseset,
									   useful_predicate,
									   ST_BITMAPSCAN,
									   NULL);
```
## Detailed Description
This function builds IndexPaths specifically for OR clause processing by examining all indexes on a relation and determining which can contribute to a bitmap OR tree. It implements a sophisticated clause matching strategy that distinguishes between "current" and "other" clauses:

- **Current clauses**: From the specific OR arm being processed
- **Other clauses**: Additional upper-level clauses that can be used for matching

The key constraint is that an index must use at least one "current" clause to be considered, preventing redundant path generation. For example, in , when processing the  subclause, an index on just  would be excluded since it would duplicate paths already generated at the upper level.

The function handles partial indexes carefully by:
1. Using predOK indexes without additional checks
2. Testing if non-predOK partial indexes have their predicates satisfied by the available clauses
3. Setting  flag when the current clauses (not just other clauses) help satisfy the predicate

## Parameters / Member Variables
- : PlannerInfo containing planner state and configuration
- : RelOptInfo representing the relation for index path generation
- : Current list of restriction clauses (RestrictInfo nodes) from the OR arm
- : List of additional upper-level clauses available for matching

## Dependencies
- Functions called/Symbols referenced:
  - [match_clauses_to_index](../m/match_clauses_to_index.md)
  - [build_index_paths](build_index_paths.md)
  - [predicate_implied_by](../p/predicate_implied_by.md)
  - [list_concat_copy](../l/list_concat_copy.md)
  - [list_concat](../l/list_concat.md)
- Called from (representative examples):
  - [generate_bitmap_or_paths](../g/generate_bitmap_or_paths.md)

## Notes and Other Information
- Only considers indexes that support bitmap scans ()
- Prevents inefficient plans by requiring current clauses to contribute to index usage
- The  flag prevents matching predOK indexes to OR arms unnecessarily
- Results are intended for bitmap OR tree construction, not regular index scans
- All generated paths use  scan type exclusively

## Simplified Source

```c
static List *
build_paths_for_OR(PlannerInfo *root, RelOptInfo *rel,
                   List *clauses, List *other_clauses)
{
    List *result = NIL;
    List *all_clauses = NIL; // computed when needed
    ListCell *lc;

    foreach(lc, rel->indexlist)
    {
        IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);
        IndexClauseSet clauseset;
        List *indexpaths;
        bool useful_predicate;

        // Skip indexes that don't support bitmap scans
        if (!index->amhasgetbitmap)
            continue;

        // Handle partial index predicates
        useful_predicate = false;
        if (index->indpred != NIL)
        {
            if (index->predOK)
            {
                // Index is usable but don't set useful_predicate
            }
            else
            {
                // Check if clauses satisfy the predicate
                if (all_clauses == NIL)
                    all_clauses = list_concat_copy(clauses, other_clauses);

                if (!predicate_implied_by(index->indpred, all_clauses, false))
                    continue; // can't use this index

                if (!predicate_implied_by(index->indpred, other_clauses, false))
                    useful_predicate = true;
            }
        }

        // Match current clauses to the index
        MemSet(&clauseset, 0, sizeof(clauseset));
        match_clauses_to_index(root, clauses, index, &clauseset);

        // Skip if no matches and predicate isn't useful
        if (!clauseset.nonempty && !useful_predicate)
            continue;

        // Add "other" restriction clauses to the clauseset
        match_clauses_to_index(root, other_clauses, index, &clauseset);

        // Build index paths for bitmap scanning
        indexpaths = build_index_paths(root, rel,
                                      index, &clauseset,
                                      useful_predicate,
                                      ST_BITMAPSCAN,
                                      NULL);
        result = list_concat(result, indexpaths);
    }

    return result;
}
```