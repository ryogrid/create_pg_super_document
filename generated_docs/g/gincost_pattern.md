# gincost_pattern

## Location
[src/backend/utils/adt/selfuncs.c:7369-7482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L7369-L7482)

## Overview
Estimates the number of index terms that need to be searched for a GIN query pattern and updates the provided cost counts accordingly.

## Definition
static bool gincost_pattern(IndexOptInfo *index, int indexcol, Oid clause_op, Datum query, GinQualCounts *counts)

## Detailed Description
The gincost_pattern function analyzes a specific GIN (Generalized Inverted Index) query pattern to estimate search costs. It calls the index's extractQuery support function to determine how many index entries need to be examined for the given query value. The function distinguishes between exact matches, partial matches, and different search modes (default, include empty, or full scan). It updates the provided GinQualCounts structure with entry counts and search mode information that will be used by the overall GIN cost estimation process. The function returns false if the query is unsatisfiable (no matches possible).

## Parameters / Member Variables
- `index`: IndexOptInfo structure containing information about the GIN index
- `indexcol`: Column number within the index being queried
- `clause_op`: OID of the operator used in the query clause
- `query`: The actual query value (constant) being searched for
- `counts`: GinQualCounts structure to be updated with cost estimation data

## Dependencies
- Functions called/Symbols referenced:
  - [get_op_opfamily_properties](get_op_opfamily_properties.md)
  - [get_opfamily_proc](get_opfamily_proc.md)
  - [get_rel_name](get_rel_name.md)
  - [fmgr_info](../f/fmgr_info.md)
  - [set_fn_opclass_options](../s/set_fn_opclass_options.md)
  - [FunctionCall7Coll](../F/FunctionCall7Coll.md)
  - GIN_EXTRACTQUERY_PROC
  - GIN_SEARCH_MODE_DEFAULT
  - GIN_SEARCH_MODE_INCLUDE_EMPTY
- Called from (representative examples):
  - [gincost_opexpr](gincost_opexpr.md)
  - [gincost_scalararrayopexpr](gincost_scalararrayopexpr.md)

## Notes and Other Information
- Uses a heuristic estimate of 100 matched entries for partial matches
- Handles three search modes: DEFAULT (normal index scan), INCLUDE_EMPTY (treats empty entries as matches), and ALL (full index scan)
- Calls the index's extractQuery support function to get actual search terms
- Updates both exact and partial entry counts for cost estimation
- Returns false for unsatisfiable queries (when nentries <= 0 in default search mode)
- Properly handles collation settings for the extractQuery function call

## Simplified Source

```c
static bool gincost_pattern(IndexOptInfo *index, int indexcol, Oid clause_op,
                           Datum query, GinQualCounts *counts) {
    FmgrInfo flinfo;
    Oid extractProcOid;
    Oid collation;
    int strategy_op;
    int32 nentries = 0;
    bool *partial_matches = NULL;
    int32 searchMode = GIN_SEARCH_MODE_DEFAULT;

    // Get operator strategy and validate
    get_op_opfamily_properties(clause_op, index->opfamily[indexcol], false,
                              &strategy_op, NULL, NULL);

    // Get the extractQuery support function
    extractProcOid = get_opfamily_proc(index->opfamily[indexcol],
                                      index->opcintype[indexcol],
                                      index->opcintype[indexcol],
                                      GIN_EXTRACTQUERY_PROC);
    if (!OidIsValid(extractProcOid)) {
        elog(ERROR, "missing support function %d for attribute %d of index \"%s\"",
             GIN_EXTRACTQUERY_PROC, indexcol + 1, get_rel_name(index->indexoid));
    }

    // Set up collation and call extractQuery function
    collation = OidIsValid(index->indexcollations[indexcol]) ?
                index->indexcollations[indexcol] : DEFAULT_COLLATION_OID;

    fmgr_info(extractProcOid, &flinfo);
    set_fn_opclass_options(&flinfo, index->opclassoptions[indexcol]);

    FunctionCall7Coll(&flinfo, collation, query,
                     PointerGetDatum(&nentries), UInt16GetDatum(strategy_op),
                     PointerGetDatum(&partial_matches), PointerGetDatum(NULL),
                     PointerGetDatum(NULL), PointerGetDatum(&searchMode));

    // Check if query is satisfiable
    if (nentries <= 0 && searchMode == GIN_SEARCH_MODE_DEFAULT)
        return false;

    // Count entries for cost estimation
    for (int i = 0; i < nentries; i++) {
        if (partial_matches && partial_matches[i])
            counts->partialEntries += 100;  // Heuristic estimate
        else
            counts->exactEntries++;
        counts->searchEntries++;
    }

    // Handle different search modes
    if (searchMode == GIN_SEARCH_MODE_DEFAULT) {
        counts->attHasNormalScan[indexcol] = true;
    } else if (searchMode == GIN_SEARCH_MODE_INCLUDE_EMPTY) {
        counts->attHasNormalScan[indexcol] = true;
        counts->exactEntries++;
        counts->searchEntries++;
    } else {
        counts->attHasFullScan[indexcol] = true;
    }

    return true;
}
```

**Core Logic**: Calls the GIN index's extractQuery function to determine search terms for a query pattern, estimates entry counts for cost calculation, and handles different search modes (normal, include empty, full scan).