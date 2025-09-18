# fix_opfuncids

## Location
src/backend/nodes/nodeFuncs.c: 1831 - 1837

## Overview
Calculates and sets the  field for all OpExpr nodes in an expression tree by deriving function OIDs from operator OIDs.

## Definition


## Detailed Description
The  function is responsible for populating the  field in operator expression nodes throughout an expression tree. This field stores the OID of the actual function that implements the operator, which is derived from the operator's OID ( field).

This function is typically called during query planning when PostgreSQL needs to convert abstract operator references into concrete function calls for execution. The process involves walking through the entire expression tree and updating operator nodes with their corresponding function implementation OIDs.

The function works by delegating to , which uses the expression tree walker infrastructure to traverse all nodes in the tree. The walker specifically handles:
-  nodes (standard binary/unary operators)
-  nodes (IS DISTINCT FROM operators)
-  nodes (NULLIF expressions)
-  nodes (scalar op ANY/ALL array expressions)

The modification is performed in-place, which is acceptable because the same change would be needed for any instance of a node, even if it appears multiple times due to shared structure in the parse tree.

## Parameters
- : The root node of the expression tree to process (can be any expression tree that expression_tree_walker handles)

## Dependencies
- Functions called/Symbols referenced:
  - fix_opfuncids_walker (internal walker function)

- Called from:
  - expression_planner (src/backend/optimizer/plan/planner.c:6669, 6711)
  - evaluate_expr (src/backend/optimizer/util/clauses.c:4993)
  - get_relation_statistics (src/backend/optimizer/util/plancat.c:1537)
  - operator_predicate_proof (src/backend/optimizer/util/predtest.c:1998)
  - Various partitioning and statistics functions
  - Cache management functions (relcache.c, partcache.c)

## Notes and Other Information
- Modifies the input tree in-place rather than creating a copy
- Used during query planning phase to prepare operators for execution
- Essential for converting parse-time operator representations to execution-time function calls
- The walker function handles struct-equivalent node types (DistinctExpr, NullIfExpr) by casting to OpExpr
- ScalarArrayOpExpr requires special handling via set_sa_opfuncid function
- Safe to call multiple times on the same tree due to idempotent nature of the operation
- Part of PostgreSQL's operator resolution and function lookup system
- Located in src/backend/nodes/nodeFuncs.c:1831-1837