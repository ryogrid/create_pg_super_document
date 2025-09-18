# RelationGetIndexExpressions

## Location
src/backend/utils/cache/relcache.c: 5043 - 5101

## Overview
RelationGetIndexExpressions retrieves and processes the expression tree for expression-based index columns, returning a parsed and optimized list of expressions used in the index definition.

## Definition
```c
List *RelationGetIndexExpressions(Relation relation)
```

## Detailed Description
This function extracts, parses, and optimizes index expressions for indexes that contain expressional columns (e.g., CREATE INDEX ON table ((expression))). It handles the complex process of converting the stored textual representation in pg_index.indexprs into executable expression trees.

The function operates through several steps:
1. Returns cached result if already computed (rd_indexprs)
2. Returns NIL for non-index relations or indexes without expressions
3. Retrieves the raw expression string from pg_index.indexprs
4. Parses the string into a node tree using stringToNode
5. Applies constant folding optimizations via eval_const_expressions
6. Fixes operator function IDs for proper execution
7. Caches the result in the relation's index context
8. Returns a copy to the caller to prevent relcache invalidation issues

The optimization step is crucial because the planner compares these expressions with query qual clauses, and unoptimized expressions might prevent the detection of valid index matches.

## Parameters / Member Variables
- `relation`: The index relation for which to retrieve index expressions

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - heap_attisnull
  - heap_getattr
  - GetPgIndexDescriptor
  - TextDatumGetCString
  - stringToNode
  - eval_const_expressions
  - fix_opfuncids
- Called from (representative examples):
  - GetIndexInputType
  - BuildIndexInfo
  - ATExecReplicaIdentity
  - index_unchanged_by_update
  - plan_create_index_workers
  - get_relation_info

## Notes and Other Information
- Returns NIL for relations that are not indexes or indexes without expressional columns
- The returned tree is always a copy to prevent issues with relcache invalidation
- Uses eval_const_expressions for optimization, but NOT canonicalize_qual since these aren't qual expressions
- Expressions are cached in rd_indexprs after first computation
- Critical for functional indexes and expression-based indexing in PostgreSQL
- The function builds results in caller's context first, then caches to avoid partial state on errors
- Located in src/backend/utils/cache/relcache.c:5043-5101