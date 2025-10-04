# searchRangeTableForRel

## Location
[src/backend/parser/parse_relation.c:356-433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L356-L433)

## Overview
Searches all RangeTblEntry items in the ParseState's range table to find one that could possibly match the given RangeVar, used primarily for error message generation.

## Definition
```c
static RangeTblEntry *searchRangeTableForRel(ParseState *pstate, RangeVar *relation)
```

## Detailed Description
This function performs a comprehensive search through all entries in the ParseState's range table(s) to find a RangeTblEntry that could match the given RangeVar. Unlike `refnameNamespaceItem`, this function considers every entry in the range table, not just those currently visible in the namespace list. This behavior is intentionally non-standard (violates SQL spec) and may return ambiguous results, so it should ONLY be used as a heuristic for generating suitable error messages.

The function checks for matches in the following order:
1. For unqualified names, first checks for CTE (Common Table Expression) matches
2. If no CTE found, checks for ENR (Ephemeral Named Relation) matches  
3. If neither CTE nor ENR found, looks up the relation ID using RangeVarGetRelid
4. Finally searches through all RTEs for matches on relation ID, CTE name, ENR name, or alias

## Parameters / Member Variables
- `pstate`: Pointer to the ParseState structure containing the range table and parsing context
- `relation`: Pointer to the RangeVar structure representing the relation reference to search for

## Dependencies
- Functions called/Symbols referenced:
  - [scanNameSpaceForCTE](scanNameSpaceForCTE.md)
  - [scanNameSpaceForENR](scanNameSpaceForENR.md)
  - RangeVarGetRelid
  - strcmp/lfirst (list operations)
- Types referenced:
  - [RangeVar](../R/RangeVar.md)
  - CommonTableExpr
  - RTE_RELATION
  - RTE_CTE
  - RTE_NAMEDTUPLESTORE
- Called from (representative examples):
  - [errorMissingRTE](../e/errorMissingRTE.md) (src/backend/parser/parse_relation.c:3604)

## Notes and Other Information
- This is a static function, not exposed in public headers
- Used specifically for error message generation in `errorMissingRTE`
- The function intentionally violates SQL specification by considering all range table entries
- May return ambiguous results if multiple valid matches exist
- Performs unlocked name lookup since it's only used for error reporting
- Searches through parent ParseStates as well (via levelsup mechanism)
- CTE matches take precedence over regular relation matches for unqualified names

## Simplified Source

```c
static RangeTblEntry *
searchRangeTableForRel(ParseState *pstate, RangeVar *relation)
{
    const char *refname = relation->relname;
    Oid relId = InvalidOid;
    CommonTableExpr *cte = NULL;
    bool isenr = false;
    Index ctelevelsup = 0;
    Index levelsup;

    // For unqualified names, check for CTE/ENR matches first
    if (!relation->schemaname) {
        cte = scanNameSpaceForCTE(pstate, refname, &ctelevelsup);
        if (!cte)
            isenr = scanNameSpaceForENR(pstate, refname);
    }

    // Look up relation ID if no CTE/ENR found
    if (!cte && !isenr)
        relId = RangeVarGetRelid(relation, NoLock, true);

    // Search all RTEs in current and parent ParseStates
    for (levelsup = 0; pstate != NULL; pstate = pstate->parentParseState, levelsup++) {
        ListCell *l;

        foreach(l, pstate->p_rtable) {
            RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);

            // Check for relation match
            if (rte->rtekind == RTE_RELATION && OidIsValid(relId) && rte->relid == relId)
                return rte;

            // Check for CTE match
            if (rte->rtekind == RTE_CTE && cte != NULL &&
                rte->ctelevelsup + levelsup == ctelevelsup &&
                strcmp(rte->ctename, refname) == 0)
                return rte;

            // Check for ENR match
            if (rte->rtekind == RTE_NAMEDTUPLESTORE && isenr &&
                strcmp(rte->enrname, refname) == 0)
                return rte;

            // Check for alias match
            if (strcmp(rte->eref->aliasname, refname) == 0)
                return rte;
        }
    }

    return NULL;
}
```