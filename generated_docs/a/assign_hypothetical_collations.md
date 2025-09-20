# assign_hypothetical_collations

## Location
[src/backend/parser/parse_collate.c:955-1058](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_collate.c#L955-L1058)

## Overview
Handles collation assignment for hypothetical-set aggregates by unifying collations between paired hypothetical and aggregated arguments according to SQL standard requirements.

## Definition

```c
structure like this, and a parse-time change of
		 * collation ought to be signaled by a CollateExpr not a RelabelType
		 * (the use of RelabelType for collation marking is supposed to be a
		 * planner/executor thing only).  But we have no better alternative.
		 * In particular, injecting a CollateExpr could result in the
		 * expression being interpreted differently after dump/reload, since
		 * we might be effectively promoting an implicit collation to
		 * explicit.  This kluge is relying on ruleutils.c not printing a
		 * COLLATE clause for a RelabelType, and probably on some other
		 * fragile behaviors.
		 */
		if (OidIsValid(paircontext.collation) &&
			paircontext.collation != exprCollation((Node *) s_tle->expr))
		{
			s_tle->expr = (Expr *)
				makeRelabelType(s_tle->expr,
								exprType((Node *) s_tle->expr),
								exprTypmod((Node *) s_tle->expr),
								paircontext.collation,
								COERCE_IMPLICIT_CAST);
		}

		/*
		 * If appropriate, merge this column's collation state up to the
		 * aggregate function.
		 */
		if (merge_sort_collations)
			merge_collation_state(paircontext.collation,
								  paircontext.strength,
								  paircontext.location,
								  paircontext.collation2,
								  paircontext.location2,
								  loccontext);
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
- RelabelType injection is noted as "grotty" but necessary for proper collation enforcement during sorting
- The RelabelType approach avoids changing implicit collations to explicit ones during dump/reload
- Examples of hypothetical-set aggregates include rank, dense_rank, percent_rank, and cume_dist functions
- Collation conflicts between paired arguments are reported immediately rather than deferred