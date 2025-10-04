# errorMissingRTE

## Location
[src/backend/parser/parse_relation.c:3594-3664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3594-L3664)

## Overview
Generates detailed error messages when a referenced table or relation is missing from the FROM clause, providing context-specific hints to help users identify and correct the issue.

## Definition

```c
void
errorMissingRTE(ParseState *pstate, RangeVar *relation)
```
## Detailed Description
This function is responsible for generating helpful error messages when PostgreSQL's parser encounters a reference to a table or relation that cannot be found in the current query's range table. The function performs sophisticated analysis to determine the most likely cause of the error and provides specific hints to guide users toward the correct syntax.

The function implements three levels of error detection and reporting:
1. **Alias confusion detection**: Identifies cases where users reference a table by its real name instead of its alias
2. **Scope violation detection**: Catches attempts to reference tables outside their valid scope (like MySQL-style JOIN syntax)  
3. **Missing table detection**: Handles cases where the table simply doesn't exist in the FROM clause

The function leverages PostgreSQL's namespace resolution system to provide contextually appropriate error messages with precise location information.

## Parameters / Member Variables
- `*pstate`: ParseState structure containing the current parsing context and namespace information
- `*relation`: RangeVar structure representing the problematic table reference that couldn't be resolved
## Dependencies
- Functions called/Symbols referenced:
  - [searchRangeTableForRel](../s/searchRangeTableForRel.md)
  - [refnameNamespaceItem](../r/refnameNamespaceItem.md)  
  - [rte_visible_if_lateral](../r/rte_visible_if_lateral.md)
  - ereport
  - [errcode](errcode.md) (ERRCODE_UNDEFINED_TABLE)
  - [errmsg](errmsg.md)
  - [errhint](errhint.md)
  - [errdetail](errdetail.md)
  - [parser_errposition](../p/parser_errposition.md)
- Called from (representative examples):
  - Various parser functions when table resolution fails

## Notes and Other Information
- The function works particularly hard to provide helpful messages since missing RTE errors are very common
- Implements special handling for alias-related confusion, which is a frequent user mistake
- Uses location information to provide precise error positioning in the source query
- Supports LATERAL join hint suggestions when appropriate
- Part of PostgreSQL's comprehensive error reporting system that aims to guide users toward correct SQL syntax

## Simplified Source

```c
void
errorMissingRTE(ParseState *pstate, RangeVar *relation)
{
    RangeTblEntry *rte;
    const char *badAlias = NULL;

    // Search for potential matches in the range table
    rte = searchRangeTableForRel(pstate, relation);

    // Check if the problem is using real name instead of alias
    if (rte && rte->alias &&
        strcmp(rte->eref->aliasname, relation->relname) != 0) {
        ParseNamespaceItem *nsitem;
        int sublevels_up;

        nsitem = refnameNamespaceItem(pstate, NULL, rte->eref->aliasname,
                                      relation->location, &sublevels_up);
        if (nsitem && nsitem->p_rte == rte)
            badAlias = rte->eref->aliasname;
    }

    // Generate appropriate error message with hints
    if (badAlias) {
        // User likely forgot to use alias
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("invalid reference to FROM-clause entry for table \"%s\"",
                        relation->relname),
                 errhint("Perhaps you meant to reference the table alias \"%s\".",
                         badAlias),
                 parser_errposition(pstate, relation->location)));
    } else if (rte) {
        // Table exists but is out of scope
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("invalid reference to FROM-clause entry for table \"%s\"",
                        relation->relname),
                 errdetail("There is an entry for table \"%s\", but it cannot be referenced from this part of the query.",
                           rte->eref->aliasname),
                 rte_visible_if_lateral(pstate, rte) ?
                 errhint("To reference that table, you must mark this subquery with LATERAL.") : 0,
                 parser_errposition(pstate, relation->location)));
    } else {
        // Table simply doesn't exist in FROM clause
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("missing FROM-clause entry for table \"%s\"",
                        relation->relname),
                 parser_errposition(pstate, relation->location)));
    }
}
```