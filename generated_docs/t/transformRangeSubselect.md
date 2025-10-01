# transformRangeSubselect

## Location
[src/backend/parser/parse_clause.c:407-464](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L407-L464)

## Overview
Transforms a sub-SELECT appearing in a FROM clause into a ParseNamespaceItem, handling LATERAL references and parsing the subquery with appropriate context.

## Definition
static ParseNamespaceItem *
transformRangeSubselect(ParseState *pstate, RangeSubselect *r)

## Detailed Description
The transformRangeSubselect function handles the transformation of subqueries that appear in FROM clauses. This function manages the complex parsing context required for subselects, including proper handling of LATERAL references and expression kind tracking. It sets up the appropriate parsing environment, calls parse_sub_analyze to process the subquery, validates that the result is a SELECT command, and finally creates the appropriate range table entry for the subquery. The function carefully manages parsing state to ensure that LATERAL references are properly resolved and that nested parsing contexts are correctly established.

## Parameters / Member Variables
- pstate: ParseState structure containing the current parsing context and state information
- r: RangeSubselect structure representing the subquery to be transformed, containing the subquery node, alias, and lateral flag

## Dependencies
- Functions called/Symbols referenced:
  - [parse_sub_analyze](../p/parse_sub_analyze.md)
  - [isLockedRefname](../i/isLockedRefname.md)
  - [addRangeTableEntryForSubquery](../a/addRangeTableEntryForSubquery.md)
  - EXPR_KIND_NONE
  - EXPR_KIND_FROM_SUBSELECT
  - CMD_SELECT
- Called from (representative examples):
  - [transformFromClauseItem](transformFromClauseItem.md)

## Notes and Other Information
- The function temporarily modifies pstate->p_expr_kind to EXPR_KIND_FROM_SUBSELECT to indicate recursive parsing into a subselect
- LATERAL functionality is handled by setting pstate->p_lateral_active when r->lateral is true
- The function includes assertions to ensure proper nesting - no nested lateral references within a single pstate level
- Lock checking is performed using isLockedRefname, considering whether the subquery has an explicit alias
- The function validates that the parsed subquery is indeed a SELECT command, as other command types should be impossible in FROM clause context
- State restoration (p_lateral_active = false, p_expr_kind = EXPR_KIND_NONE) ensures clean parsing context for subsequent operations

## Simplified Source

```c
static ParseNamespaceItem *transformRangeSubselect(ParseState *pstate, RangeSubselect *r)
{
    Query *query;

    // Set expression kind to indicate subselect parsing
    Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
    pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

    // Enable lateral references if LATERAL is specified
    Assert(!pstate->p_lateral_active);
    pstate->p_lateral_active = r->lateral;

    // Parse the subquery with proper locking context
    query = parse_sub_analyze(r->subquery, pstate, NULL,
                             isLockedRefname(pstate,
                                           r->alias == NULL ? NULL :
                                           r->alias->aliasname),
                             true);

    // Restore parsing state
    pstate->p_lateral_active = false;
    pstate->p_expr_kind = EXPR_KIND_NONE;

    // Validate that we got a SELECT command
    if (!IsA(query, Query) || query->commandType != CMD_SELECT)
        elog(ERROR, "unexpected non-SELECT command in subquery in FROM");

    // Create range table entry for the subquery
    return addRangeTableEntryForSubquery(pstate, query, r->alias,
                                        r->lateral, true);
}
```