# applyRelabelType

## Location
src/backend/nodes/nodeFuncs.c: 631 - 683

## Overview
Adds a RelabelType node if needed to make an expression expose the specified type, typmod, and collation, while maintaining post-optimization invariants.

## Definition


## Detailed Description
The  function is a smart constructor for RelabelType nodes that adds type relabeling only when necessary. It's designed for use during query planning and maintains important invariants:

1. **No adjacent RelabelTypes**: If it finds stacked RelabelType nodes, it discards all but the top one to ensure semantically equivalent expressions are equal()
2. **Const-folded tree**: It never returns a RelabelType atop a Const node; instead, it modifies the Const directly
3. **Optimization**: If the expression already has the target type, typmod, and collation, it returns the original expression unchanged

The function handles three cases:
- **Const nodes**: Modifies the Const's type information directly (with optional copying based on overwrite_ok)
- **Already correct type**: Returns the original expression if type, typmod, and collation already match
- **Needs relabeling**: Creates a new RelabelType node with the specified parameters

This function is crucial for maintaining the integrity of the expression tree during optimization while ensuring minimal overhead.

## Parameters / Member Variables
- : The expression node to potentially relabel
- : Target result type (Oid)
- : Target result type modifier
- : Target result collation
- : Coercion format specification
- : Location in source for error reporting
- : Whether it's safe to modify Const nodes in-place (true only if Const is newly generated)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for node type checking)
  - copyObject (for copying Const nodes when overwrite_ok is false)
  - exprType, exprTypmod, exprCollation (for checking current expression properties)
  - makeNode (for creating new RelabelType nodes)

- Called from (representative examples):
  - relabel_to_typmod (wrapper for typmod-only relabeling)
  - coerce_type_typmod (in type coercion system)
  - eval_const_expressions_mutator (during constant folding)
  - canonicalize_ec_expression (in equivalence class processing)
  - generate_setop_tlist (in set operation planning)

## Notes and Other Information
- Primarily intended for use during query planning
- Maintains post-eval_const_expressions invariants critical for optimization
- Strips nested RelabelType nodes to ensure expression equality works correctly
- For Const nodes, preserves the original location rather than using rlocation
- The overwrite_ok parameter should only be true when the caller knows the Const is newly generated
- Essential for PostgreSQL's type coercion system and expression optimization
- Located in src/backend/nodes/nodeFuncs.c:631-683