# isFutureCTE

## Location
[src/backend/parser/parse_relation.c:313-334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L313-L334)

## Overview
Checks if a given reference name matches a "future CTE" that is not yet in scope according to WITH scoping rules, used for improved error reporting.

## Definition

```c
static bool
isFutureCTE(ParseState *pstate, const char *refname)
```
## Detailed Description
This static function searches through the p_future_ctes lists in the parsing state hierarchy to determine if a reference name corresponds to a Common Table Expression that will be defined later in the query but is not yet accessible due to SQL scoping rules. This function is specifically designed for error reporting purposes - it helps PostgreSQL provide more helpful error messages when users reference CTEs that exist but are not yet in scope, rather than simply reporting "relation does not exist".

## Parameters / Member Variables
- : Current parsing state containing future CTE information
- : The CTE name to check for future definition

## Dependencies
- Functions called/Symbols referenced:
  - CommonTableExpr (struct type)
  - strcmp (for name comparison)
- Called from (representative examples):
  - [parserOpenTable](../p/parserOpenTable.md)

## Notes and Other Information
- Searches p_future_ctes list rather than p_ctenamespace
- Traverses the parsing state hierarchy using parentParseState links
- Returns true if the name matches any future CTE, false otherwise
- Not related to valid SQL semantics but crucial for user-friendly error messages
- Part of PostgreSQL's enhanced error reporting system for WITH clause references
- Helps distinguish between truly non-existent relations and incorrectly scoped CTE references

## Simplified Source

```c
static bool
isFutureCTE(ParseState *pstate, const char *refname)
{
    // Search through parse state hierarchy for future CTEs
    for (; pstate != NULL; pstate = pstate->parentParseState) {
        ListCell *lc;

        // Check each future CTE in current parse state
        foreach(lc, pstate->p_future_ctes) {
            CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

            if (strcmp(cte->ctename, refname) == 0)
                return true; // Found matching future CTE
        }
    }

    return false; // No matching future CTE found
}
```