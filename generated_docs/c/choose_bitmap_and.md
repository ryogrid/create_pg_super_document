# choose_bitmap_and

## Location
[src/backend/optimizer/path/indxpath.c:1287-1492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L1287-L1492)

## Overview
 takes a list of bitmap paths and intelligently combines them into a single path, choosing an optimal subset to balance selectivity and computational cost.

## Definition

```c
static Path *
choose_bitmap_and(PlannerInfo *root, RelOptInfo *rel, List *paths)
```
## Detailed Description
This sophisticated optimization function implements a multi-stage heuristic algorithm to select the best combination of bitmap index paths for AND operations:

**Stage 1 - Deduplication**: Groups paths that use identical sets of WHERE clauses and index predicates, keeping only the cheapest-to-scan path in each group. This eliminates redundant indexes that include the same interesting columns plus irrelevant ones.

**Stage 2 - Sorting**: Sorts remaining paths by index access cost with cheapest first.

**Stage 3 - Combination Selection**: Uses an O(N²) algorithm rather than exhaustive O(2^N) enumeration:
- For each path as a potential "AND group leader"
- Progressively considers adding subsequent (higher-cost) paths
- Keeps additions only if they reduce the estimated total scan cost
- Maintains the cheapest combination found

**Redundancy Prevention**: Implements strict redundancy checks to avoid double-counting selectivity:
- **Clause Redundancy**: Rejects combinations where indexes use the same WHERE clauses
- **Predicate Redundancy**: Rejects partial indexes whose predicate clauses are implied by already-selected conditions

The function handles edge cases efficiently, returning single paths unchanged and creating  objects only when multiple paths are beneficial.

## Parameters / Member Variables
- `*root`: PlannerInfo containing planner state and configuration
- `*rel`: RelOptInfo representing the relation being scanned
- `*paths`: List of candidate bitmap paths to potentially combine
## Dependencies
- Functions called/Symbols referenced:
  - [classify_index_clause_usage](classify_index_clause_usage.md)
  - [cost_bitmap_tree_node](cost_bitmap_tree_node.md)
  - [bitmap_scan_cost_est](../b/bitmap_scan_cost_est.md)
  - [bitmap_and_cost_est](../b/bitmap_and_cost_est.md)
  - [create_bitmap_and_path](create_bitmap_and_path.md)
  - [predicate_implied_by](../p/predicate_implied_by.md)
  - [path_usage_comparator](../p/path_usage_comparator.md)
- Called from (representative examples):
  - [generate_bitmap_or_paths](../g/generate_bitmap_or_paths.md)
  - [create_index_paths](create_index_paths.md)

## Notes and Other Information
- Returns either a single input path or a new BitmapAndPath combining multiple inputs
- The O(N²) complexity makes it practical even with many candidate paths after deduplication
- Redundancy detection prevents costsize.c and clausesel.c from double-counting clauses
- Handles both regular WHERE clause redundancy and index predicate clause implications
- The prefiltering stage can dramatically reduce path count in systems with many variant indexes
- Uses  structures to efficiently track clause usage patterns

## Simplified Source

```c
static Path *
choose_bitmap_and(PlannerInfo *root, RelOptInfo *rel, List *paths)
{
    int npaths = list_length(paths);
    PathClauseUsage **pathinfoarray;
    List *bestpaths = NIL;
    Cost bestcost = 0;

    if (npaths == 1)
        return (Path *) linitial(paths);

    // Stage 1: Deduplicate paths with identical clause sets
    // Keep only cheapest path in each group
    pathinfoarray = (PathClauseUsage **) palloc(npaths * sizeof(PathClauseUsage *));
    List *clauselist = NIL;
    npaths = 0;

    foreach(ListCell *l, paths)
    {
        Path *ipath = (Path *) lfirst(l);
        PathClauseUsage *pathinfo = classify_index_clause_usage(ipath, &clauselist);

        if (pathinfo->unclassifiable)
        {
            pathinfoarray[npaths++] = pathinfo;
            continue;
        }

        // Look for duplicate clause sets
        int i;
        for (i = 0; i < npaths; i++)
        {
            if (!pathinfoarray[i]->unclassifiable &&
                bms_equal(pathinfo->clauseids, pathinfoarray[i]->clauseids))
                break;
        }

        if (i < npaths)
        {
            // Found duplicate - keep cheaper one
            Cost ncost, ocost;
            Selectivity nselec, oselec;
            cost_bitmap_tree_node(pathinfo->path, &ncost, &nselec);
            cost_bitmap_tree_node(pathinfoarray[i]->path, &ocost, &oselec);
            if (ncost < ocost)
                pathinfoarray[i] = pathinfo;
        }
        else
        {
            // Not duplicate - add to array
            pathinfoarray[npaths++] = pathinfo;
        }
    }

    if (npaths == 1)
        return pathinfoarray[0]->path;

    // Stage 2: Sort by index access cost
    qsort(pathinfoarray, npaths, sizeof(PathClauseUsage *), path_usage_comparator);

    // Stage 3: Try each path as AND group leader
    // Use O(N²) algorithm instead of O(2^N) exhaustive search
    for (int i = 0; i < npaths; i++)
    {
        PathClauseUsage *pathinfo = pathinfoarray[i];
        List *current_paths = list_make1(pathinfo->path);
        Cost costsofar = bitmap_scan_cost_est(root, rel, pathinfo->path);
        List *qualsofar = list_concat_copy(pathinfo->quals, pathinfo->preds);
        Bitmapset *clauseidsofar = bms_copy(pathinfo->clauseids);

        // Try adding subsequent higher-cost paths
        for (int j = i + 1; j < npaths; j++)
        {
            pathinfo = pathinfoarray[j];

            // Check for redundancy - avoid double-counting selectivity
            if (bms_overlap(pathinfo->clauseids, clauseidsofar))
                continue;

            // Check if any predicate clauses are implied by existing ones
            if (pathinfo->preds)
            {
                bool redundant = false;
                foreach(ListCell *l, pathinfo->preds)
                {
                    Node *np = (Node *) lfirst(l);
                    if (predicate_implied_by(list_make1(np), qualsofar, false))
                    {
                        redundant = true;
                        break;
                    }
                }
                if (redundant)
                    continue;
            }

            // Test adding this path to the combination
            current_paths = lappend(current_paths, pathinfo->path);
            Cost newcost = bitmap_and_cost_est(root, rel, current_paths);

            if (newcost < costsofar)
            {
                // Keep new path - it reduces total cost
                costsofar = newcost;
                qualsofar = list_concat(qualsofar, pathinfo->quals);
                qualsofar = list_concat(qualsofar, pathinfo->preds);
                clauseidsofar = bms_add_members(clauseidsofar, pathinfo->clauseids);
            }
            else
            {
                // Reject new path - remove from combination
                current_paths = list_truncate(current_paths, list_length(current_paths) - 1);
            }
        }

        // Keep track of cheapest combination found
        if (i == 0 || costsofar < bestcost)
        {
            bestpaths = current_paths;
            bestcost = costsofar;
        }
    }

    // Return single path or AND combination
    if (list_length(bestpaths) == 1)
        return (Path *) linitial(bestpaths);
    return (Path *) create_bitmap_and_path(root, rel, bestpaths);
}
```