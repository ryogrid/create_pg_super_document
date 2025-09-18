# RelationGetIndexPredicate

## Location
[src/backend/utils/cache/relcache.c:5156-5248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L5156-L5248)

## Overview
RelationGetIndexPredicate retrieves and processes the predicate (WHERE clause) for a partial index, returning an optimized and canonicalized expression tree suitable for query planning.

## Definition
```c
List *RelationGetIndexPredicate(Relation relation)
```

## Detailed Description
This function extracts, parses, and optimizes the predicate expression for partial indexes (indexes created with a WHERE clause). It performs comprehensive processing to ensure the predicate is in the optimal format for the query planner to use when determining whether the index can satisfy query conditions.

The function operates through several critical steps:
1. Returns cached result if already computed (rd_indpred)
2. Returns NIL for non-index relations or indexes without predicates
3. Retrieves the raw predicate string from pg_index.indpred
4. Parses the string into a node tree using stringToNode
5. Applies constant expression evaluation via eval_const_expressions
6. Canonicalizes the expression using canonicalize_qual for consistent formatting
7. Converts to implicit-AND format for efficient planner operations
8. Fixes operator function IDs for proper execution
9. Caches the result in the relation's index context
10. Returns a copy to prevent relcache invalidation issues

The optimization and canonicalization steps are essential because the planner compares these predicates with query WHERE clauses to determine index usability. Without proper processing, the planner might fail to recognize when a partial index is applicable to a query.

## Parameters / Member Variables
- `relation`: The index relation for which to retrieve the predicate

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - [heap_attisnull](../h/heap_attisnull.md)
  - [heap_getattr](../h/heap_getattr.md)
  - [GetPgIndexDescriptor](../G/GetPgIndexDescriptor.md)
  - TextDatumGetCString
  - [stringToNode](../s/stringToNode.md)
  - [eval_const_expressions](../e/eval_const_expressions.md)
  - [canonicalize_qual](../c/canonicalize_qual.md)
  - [make_ands_implicit](../m/make_ands_implicit.md)
  - [fix_opfuncids](../f/fix_opfuncids.md)
- Called from (representative examples):
  - [BuildIndexInfo](../B/BuildIndexInfo.md)
  - [is_usable_unique_index](../i/is_usable_unique_index.md)
  - [ATExecReplicaIdentity](../A/ATExecReplicaIdentity.md)
  - [plan_create_index_workers](../p/plan_create_index_workers.md)
  - [get_relation_info](../g/get_relation_info.md)
  - [infer_arbiter_indexes](../i/infer_arbiter_indexes.md)

## Notes and Other Information
- Returns NIL for relations that are not indexes or indexes without predicates (full indexes)
- The processing must match what's done to qual clauses in preprocess_expression()
- Unlike regular expressions, subqueries are not allowed in index predicates
- Results are cached in rd_indpred after first computation for performance
- The returned tree is always a copy to prevent relcache invalidation issues
- Critical for partial index optimization and query plan generation
- The implicit-AND format simplifies planner logic for predicate matching
- Located in src/backend/utils/cache/relcache.c:5156-5248