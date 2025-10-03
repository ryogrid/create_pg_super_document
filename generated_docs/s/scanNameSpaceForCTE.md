# scanNameSpaceForCTE

## Location
[src/backend/parser/parse_relation.c:282-312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L282-L312)

## Overview
Searches through the CTE (Common Table Expression) namespace hierarchy to find a CTE matching a given unqualified reference name.

## Definition

```c
CommonTableExpr *
scanNameSpaceForCTE(ParseState *pstate, const char *refname,
					Index *ctelevelsup)
```
## Detailed Description
This function searches through the CTE namespace starting from the current parsing state and traversing up through parent parsing states to find a Common Table Expression that matches the given reference name. It implements the scoping rules for CTEs, where inner scopes can reference CTEs defined in outer scopes. The function returns both the matching CTE and the nesting level where it was found. Unlike relation namespace searches, this function doesn't need to handle ambiguity since parse_cte.c ensures CTE names are unique within each WITH clause.

## Parameters / Member Variables
- `*pstate`: Current parsing state containing CTE namespace information
- `*refname`: The unqualified CTE name to search for
- `*ctelevelsup`: Output parameter that receives the nesting level where the CTE was found
## Dependencies
- Functions called/Symbols referenced:
  - CommonTableExpr (struct type)
  - strcmp (for name comparison)
- Called from (representative examples):
  - [getNSItemForSpecialRelationTypes](../g/getNSItemForSpecialRelationTypes.md)
  - [searchRangeTableForRel](searchRangeTableForRel.md)

## Notes and Other Information
- Traverses the parsing state hierarchy using parentParseState links
- Searches p_ctenamespace list at each level
- Returns the levelsup count indicating how many parsing levels up the CTE was found
- No ambiguity handling needed since CTE names must be unique within each WITH clause
- Part of PostgreSQL's CTE resolution system for recursive and non-recursive common table expressions

## Simplified Source

```c
CommonTableExpr *
scanNameSpaceForCTE(ParseState *pstate, const char *refname,
                    Index *ctelevelsup)
{
    Index levelsup;

    // Search through parsing state hierarchy for matching CTE
    for (levelsup = 0; pstate != NULL; pstate = pstate->parentParseState, levelsup++)
    {
        ListCell *lc;

        // Check all CTEs in current namespace level
        foreach(lc, pstate->p_ctenamespace)
        {
            CommonTableExpr *cte = lfirst(lc);

            if (strcmp(cte->ctename, refname) == 0)
            {
                *ctelevelsup = levelsup;
                return cte;
            }
        }
    }
    return NULL;
}
```