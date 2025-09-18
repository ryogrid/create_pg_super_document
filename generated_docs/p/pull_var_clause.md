# pull_var_clause

## Location
src/backend/optimizer/util/var.c: 607 - 626

## Overview
Recursively extracts all Var nodes (and optionally other node types) from an expression clause, with configurable handling of aggregates, window functions, and placeholders.

## Definition
```c
List *pull_var_clause(Node *node, int flags)
```

## Detailed Description
This function traverses an expression tree and collects all Var nodes into a list, with sophisticated handling of complex node types based on the provided flags. It serves as the main entry point for variable extraction in the PostgreSQL optimizer.

The function provides flexible control over how different node types are handled:

**Aggregate Functions (Aggrefs):**
- `PVC_INCLUDE_AGGREGATES`: Include Aggref nodes in the output list
- `PVC_RECURSE_AGGREGATES`: Recurse into Aggref arguments to find Vars
- Neither flag: Throw error if Aggref found

**Window Functions:**
- `PVC_INCLUDE_WINDOWFUNCS`: Include WindowFunc nodes in the output list  
- `PVC_RECURSE_WINDOWFUNCS`: Recurse into WindowFunc arguments to find Vars
- Neither flag: Throw error if WindowFunc found

**PlaceHolderVars:**
- `PVC_INCLUDE_PLACEHOLDERS`: Include PlaceHolderVar nodes in the output list
- `PVC_RECURSE_PLACEHOLDERS`: Recurse into PlaceHolderVar arguments to find Vars
- Neither flag: Throw error if PlaceHolderVar found

The function includes assertions to prevent conflicting flag combinations (e.g., both INCLUDE and RECURSE flags for the same node type).

Important constraints:
- Must only be used after sublinks have been reduced to subplans
- Does not examine subqueries
- Upper-level vars (varlevelsup > 0) should not be present
- CurrentOfExpr nodes are ignored
- Returns references to original nodes, not copies

## Parameters / Member Variables
- `node`: The root node of the expression tree to search
- `flags`: Bitmask controlling behavior for different node types (PVC_* constants)

## Dependencies
- Functions called/Symbols referenced:
  - [pull_var_clause_walker](pull_var_clause_walker.md) (the actual tree walker implementation)
  - Assert (for validating flag combinations)
- Constants used:
  - PVC_INCLUDE_AGGREGATES, PVC_RECURSE_AGGREGATES
  - PVC_INCLUDE_WINDOWFUNCS, PVC_RECURSE_WINDOWFUNCS  
  - PVC_INCLUDE_PLACEHOLDERS, PVC_RECURSE_PLACEHOLDERS
- Data structures used:
  - pull_var_clause_context (context structure with varlist and flags)
- Called from (examples):
  - [StoreRelCheck](../S/StoreRelCheck.md) (src/backend/catalog/heap.c:2152)
  - [qual_is_pushdown_safe](../q/qual_is_pushdown_safe.md) (src/backend/optimizer/path/allpaths.c:3888)
  - [build_base_rel_tlists](../b/build_base_rel_tlists.md) (src/backend/optimizer/plan/initsplan.c:236)
  - [preprocess_targetlist](preprocess_targetlist.md) (src/backend/optimizer/prep/preptlist.c:166)

## Notes and Other Information
- Returns a List of Node pointers - the actual nodes, not copies
- GroupingFuncs are treated exactly like Aggrefs and use the same flags
- The function validates that conflicting flags are not specified (INCLUDE and RECURSE for the same node type)
- Critical restriction: must only be called after sublink reduction, as it does not handle subqueries
- Widely used throughout the optimizer for extracting variable references from expressions
- The context structure contains: `varlist` (output list, initialized to NIL) and `flags` (copy of input flags)