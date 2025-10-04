# searchRangeTableForCol

## Location
[src/backend/parser/parse_relation.c:952-1034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L952-L1034)

## Overview
Searches all range table entries for a given column name to find the best match available, including approximate matches for error reporting purposes.

## Definition

```c
union(var->varnullingrels, relids);
```
## Detailed Description
The `searchRangeTableForCol` function performs a comprehensive search through all range table entries in the parser state hierarchy, unlike `colNameToVar` which only considers currently visible namespace items. This function is specifically designed for error reporting and diagnostic purposes, not for normal column resolution. It violates SQL spec behavior by considering all range table entries regardless of visibility rules.

The function supports both exact and approximate matching using fuzzy string matching algorithms. It calculates Levenshtein distances to find close matches when exact matches are not available, which is useful for providing helpful error messages when users make typos in column or alias names. The function excludes JOIN range table entries from consideration as they duplicate other RTEs and would produce unhelpful diagnostic messages.

## Parameters / Member Variables
- `pstate`: The parse state containing range tables to search
- `alias`: Optional alias name to match against (can be NULL)
- `colname`: The column name to search for
- `location`: Source location for error reporting purposes

## Dependencies
- Functions called/Symbols referenced:
  - [FuzzyAttrMatchState](../F/FuzzyAttrMatchState.md)
  - [varstr_levenshtein_less_equal](../v/varstr_levenshtein_less_equal.md)
  - [scanRTEForColumn](scanRTEForColumn.md)
  - MAX_FUZZY_DISTANCE
  - RTE_JOIN
  - InvalidAttrNumber
- Called from (representative examples):
  - [errorMissingColumn](../e/errorMissingColumn.md)

## Notes and Other Information
- This function is intended ONLY for error reporting heuristics, not normal column resolution
- Returns a FuzzyAttrMatchState struct containing information about exact and approximate matches
- Uses Levenshtein distance calculation for fuzzy matching with a maximum distance threshold
- Skips JOIN range table entries to avoid unhelpful alias names in error messages
- May return ambiguous results since it considers all range table entries regardless of SQL visibility rules
- The function traverses the entire parse state hierarchy, examining parent parse states as well

## Simplified Source

```c
static FuzzyAttrMatchState *
searchRangeTableForCol(ParseState *pstate, const char *alias, const char *colname,
                       int location)
{
    ParseState *orig_pstate = pstate;
    FuzzyAttrMatchState *fuzzystate = palloc(sizeof(FuzzyAttrMatchState));

    // Initialize fuzzy match state
    fuzzystate->distance = MAX_FUZZY_DISTANCE + 1;
    fuzzystate->rfirst = NULL;
    fuzzystate->rsecond = NULL;
    fuzzystate->rexact1 = NULL;
    fuzzystate->rexact2 = NULL;

    // Search through all parse states in hierarchy
    while (pstate != NULL) {
        ListCell *l;

        foreach(l, pstate->p_rtable) {
            RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);
            int fuzzy_rte_penalty = 0;
            int attnum;

            // Skip JOIN RTEs - they duplicate other RTEs
            if (rte->rtekind == RTE_JOIN)
                continue;

            // Calculate fuzzy match penalty for alias if provided
            if (alias != NULL)
                fuzzy_rte_penalty = varstr_levenshtein_less_equal(
                    alias, strlen(alias),
                    rte->eref->aliasname, strlen(rte->eref->aliasname),
                    1, 1, 1, MAX_FUZZY_DISTANCE + 1, true);

            // Scan RTE for matching column
            attnum = scanRTEForColumn(orig_pstate, rte, rte->eref,
                                    colname, location,
                                    fuzzy_rte_penalty, fuzzystate);

            // Handle exact matches
            if (attnum != InvalidAttrNumber && fuzzy_rte_penalty == 0) {
                if (fuzzystate->rexact1 == NULL) {
                    fuzzystate->rexact1 = rte;
                    fuzzystate->exact1 = attnum;
                } else {
                    fuzzystate->rexact2 = rte;
                    fuzzystate->exact2 = attnum;
                }
            }
        }

        pstate = pstate->parentParseState;
    }

    return fuzzystate;
}
```