# assign_hypothetical_collations

## Location
[src/backend/parser/parse_collate.c:955-1058](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_collate.c#L955-L1058)

## Overview
Handles collation assignment for hypothetical-set aggregates by unifying collations between paired hypothetical and aggregated arguments according to SQL standard requirements.

## Definition

```c
static void
assign_hypothetical_collations(Aggref *aggref,
                              assign_collations_context *loccontext)
```
## Detailed Description
This function implements the most complex collation assignment logic for hypothetical-set aggregates (AGGKIND_HYPOTHETICAL). These aggregates require special handling because:

1. **Paired Arguments**: Hypothetical direct arguments must be unified with their corresponding aggregated arguments (e.g., in , val and col must have compatible collations)
2. **Forced Collation**: The chosen collation must be propagated down to the sort column to ensure proper sorting behavior
3. **Conditional Contribution**: Direct arguments contribute to aggregate collation only when their partner aggregated arguments do

The function processes arguments in three phases:
1. **Extra Direct Args**: Non-hypothetical direct arguments processed normally
2. **Paired Processing**: Each hypothetical/aggregated pair is unified using a local context, conflicts are immediately reported
3. **Collation Propagation**: If needed, a RelabelType node is inserted to force the unified collation on the sort column

## Parameters / Member Variables  
- `aggref`: Pointer to the Aggref node representing the hypothetical-set aggregate function call
- `loccontext`: Local collation context for accumulating collation state from qualifying argument pairs

## Dependencies
- Functions called/Symbols referenced:
  -  (for processing individual arguments)
  -  (for combining pair collation with aggregate collation)
  -  (for forcing collation on sort columns)
  - ,  (to determine merge behavior)
  - , ,  (expression introspection)
  -  (for error messages)
- Called from (representative examples):
  -  (when processing AGGKIND_HYPOTHETICAL aggregates)

## Notes and Other Information
- The merge_sort_collations flag works similarly to ordered-set aggregates (single non-variadic argument)
- [RelabelType](../R/RelabelType.md) injection is noted as "grotty" but necessary for proper collation enforcement during sorting
- The RelabelType approach avoids changing implicit collations to explicit ones during dump/reload
- Examples of hypothetical-set aggregates include rank, dense_rank, percent_rank, and cume_dist functions
- Collation conflicts between paired arguments are reported immediately rather than deferred

## Simplified Source

```c
static void
assign_hypothetical_collations(Aggref *aggref,
                              assign_collations_context *loccontext)
{
    ListCell *h_cell = list_head(aggref->aggdirectargs);
    ListCell *s_cell = list_head(aggref->args);
    bool merge_sort_collations;
    int extra_args;

    // Determine if we should merge sort collations to parent
    merge_sort_collations = (list_length(aggref->args) == 1 &&
                            get_func_variadictype(aggref->aggfnoid) == InvalidOid);

    // Process non-hypothetical direct arguments first
    extra_args = list_length(aggref->aggdirectargs) - list_length(aggref->args);
    Assert(extra_args >= 0);
    while (extra_args-- > 0) {
        (void) assign_collations_walker((Node *) lfirst(h_cell), loccontext);
        h_cell = lnext(aggref->aggdirectargs, h_cell);
    }

    // Process paired hypothetical and aggregated arguments
    while (h_cell && s_cell) {
        Node *h_arg = (Node *) lfirst(h_cell);
        TargetEntry *s_tle = (TargetEntry *) lfirst(s_cell);
        assign_collations_context paircontext;

        // Initialize pair context for collation unification
        paircontext.pstate = loccontext->pstate;
        paircontext.collation = InvalidOid;
        paircontext.strength = COLLATE_NONE;
        paircontext.location = -1;
        paircontext.collation2 = InvalidOid;
        paircontext.location2 = -1;

        // Assign collations to both arguments
        (void) assign_collations_walker(h_arg, &paircontext);
        (void) assign_collations_walker((Node *) s_tle->expr, &paircontext);

        // Handle collation conflicts
        if (paircontext.strength == COLLATE_CONFLICT)
            ereport(ERROR, (errcode(ERRCODE_COLLATION_MISMATCH),
                           errmsg("collation mismatch between implicit collations \"%s\" and \"%s\"",
                                  get_collation_name(paircontext.collation),
                                  get_collation_name(paircontext.collation2))));

        // Force unified collation onto sort column if needed
        if (OidIsValid(paircontext.collation) &&
            paircontext.collation != exprCollation((Node *) s_tle->expr)) {
            s_tle->expr = (Expr *)
                makeRelabelType(s_tle->expr,
                               exprType((Node *) s_tle->expr),
                               exprTypmod((Node *) s_tle->expr),
                               paircontext.collation,
                               COERCE_IMPLICIT_CAST);
        }

        // Merge collation state to aggregate function if appropriate
        if (merge_sort_collations)
            merge_collation_state(paircontext.collation,
                                 paircontext.strength,
                                 paircontext.location,
                                 paircontext.collation2,
                                 paircontext.location2,
                                 loccontext);

        h_cell = lnext(aggref->aggdirectargs, h_cell);
        s_cell = lnext(aggref->args, s_cell);
    }

    Assert(h_cell == NULL && s_cell == NULL);
}
```