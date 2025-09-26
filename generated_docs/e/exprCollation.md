# exprCollation

## Location
[src/backend/nodes/nodeFuncs.c:816-1067](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L816-L1067)

## Overview
Returns the OID of the collation associated with an expression's result, handling all PostgreSQL expression node types to determine their appropriate collation properties.

## Definition
```c
Oid exprCollation(const Node *expr)
```

## Detailed Description
This comprehensive function analyzes any PostgreSQL expression node to determine the collation OID of its result value. It implements a large switch statement covering all expression node types, from simple variables and constants to complex subqueries and JSON expressions. The function distinguishes between collatable and non-collatable result types, returning InvalidOid for expressions that produce non-collatable results (like boolean, numeric, or composite types). For collatable expressions, it extracts the appropriate collation from the node's specific collation field. The function handles recursive cases by calling itself on sub-expressions and includes special logic for subqueries, alternative subplans, and JSON expressions.

## Parameters / Member Variables
- `expr`: The expression node to examine for collation information. Can be NULL, in which case InvalidOid is returned.

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (macro to get node type)
  - IsA (macro for type checking)
  - linitial_node (macro for accessing typed list elements)
  - elog (error logging function)
  - Assert (assertion macro)
  - Various expression node types (Var, Const, Param, FuncExpr, OpExpr, SubLink, etc.)
  - Collation constants (InvalidOid, C_COLLATION_OID, DEFAULT_COLLATION_OID)
  - Sublink type constants (EXPR_SUBLINK, ARRAY_SUBLINK)

- Called from (representative examples):
  - [examine_attribute](examine_attribute.md) (statistics analysis)
  - [create_ctas_nodata](../c/create_ctas_nodata.md) (CREATE TABLE AS operations)
  - [ComputeIndexAttrs](../C/ComputeIndexAttrs.md) (index attribute computation)
  - [ComputePartitionAttrs](../C/ComputePartitionAttrs.md) (partition key analysis)
  - [ExecTypeFromTLInternal](../E/ExecTypeFromTLInternal.md) (executor type handling)
  - [build_pertrans_for_aggref](../b/build_pertrans_for_aggref.md) (aggregate processing)
  - [makeVarFromTargetEntry](../m/makeVarFromTargetEntry.md) (variable creation)
  - [canonicalize_ec_expression](../c/canonicalize_ec_expression.md) (equivalence class processing)
  - [assign_collations_walker](../a/assign_collations_walker.md) (collation assignment)
  - [transformCaseExpr](../t/transformCaseExpr.md) (CASE expression transformation)
  - Self-recursive calls for nested expressions

## Notes and Other Information
- This function is central to PostgreSQL's collation system, which handles locale-specific string comparison and sorting
- The distinction between result collation and input collation is important - this function returns the collation of the expression's output
- Expression nodes that can invoke functions often have separate inputcollid fields for function parameter collation
- Non-collatable result types (boolean, numeric, composite) always return InvalidOid
- Special handling exists for subqueries, where collation comes from the first target list entry
- JSON expressions have complex collation rules depending on whether coercion is applied
- The function includes comprehensive coverage of all PostgreSQL expression node types as of the current version
- Critical for query planning, index operations, and proper string comparison semantics