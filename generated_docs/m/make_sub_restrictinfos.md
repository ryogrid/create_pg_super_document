# make_sub_restrictinfos

## Location
src/backend/optimizer/util/restrictinfo.c: 271 - 358

## Overview
Recursively inserts RestrictInfo nodes into boolean expressions, handling the proper structuring of AND/OR clause hierarchies by wrapping appropriate sub-expressions in RestrictInfo nodes.

## Definition


## Detailed Description
This static function implements the recursive logic for inserting RestrictInfo nodes at appropriate locations within complex boolean expressions. It follows PostgreSQL's design principle of using implicit-AND lists at the top level, placing RestrictInfo nodes above simple (non-AND/OR) clauses and above sub-OR clauses, but not above sub-AND clauses. For OR clauses, it recursively processes each argument and creates a RestrictInfo containing both the original clause and a reconstructed OR clause with RestrictInfo-wrapped arguments. For AND clauses, it recursively processes arguments but returns the AND clause directly without wrapping. For simple clauses, it creates a RestrictInfo directly.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and state
- : The boolean expression to process and potentially wrap with RestrictInfo nodes
- : Flag indicating whether restrictions were pushed down from higher levels
- : Flag indicating whether RestrictInfo nodes should be marked as having clones
- : Flag indicating whether RestrictInfo nodes should be marked as clones
- : Flag indicating whether the clause represents a pseudoconstant condition
- : Security level for row-level security evaluation ordering
- : Set of relations required for the top-level output (passed to OR constituents as NULL)
- : Set of relations incompatible with this restriction
- : Set of relations that are outer to this restriction context

## Dependencies
- Functions called/Symbols referenced:
  - [is_orclause](../i/is_orclause.md)
  - BoolExpr
  - [make_sub_restrictinfos](make_sub_restrictinfos.md) (recursive call)
  - [make_restrictinfo_internal](make_restrictinfo_internal.md)
  - [make_orclause](make_orclause.md)
  - [is_andclause](../i/is_andclause.md)
  - [make_andclause](make_andclause.md)
- Called from (representative examples):
  - [make_restrictinfo](make_restrictinfo.md)
  - [make_sub_restrictinfos](make_sub_restrictinfos.md) (recursive calls)

## Notes and Other Information
- Recursive structure: The function calls itself to process nested boolean expressions, ensuring proper RestrictInfo placement throughout the expression tree
- OR clause special handling: For OR clauses, creates both the original clause and a modified version where each OR argument is wrapped in RestrictInfo nodes, then wraps the entire structure in a top-level RestrictInfo
- AND clause passthrough: AND clauses are processed recursively but not wrapped in RestrictInfo nodes themselves, maintaining PostgreSQL's implicit-AND design
- Required relations handling: The top-level required_relids parameter is preserved for the final RestrictInfo, but OR constituents are allowed to default to their contained relations (passed as NULL)
- Uniform flag propagation: All boolean flags (is_pushed_down, has_clone, etc.) and metadata (security_level, incompatible_relids, outer_relids) are propagated uniformly to all created RestrictInfo nodes
- Expression tree reconstruction: The function carefully reconstructs the boolean expression structure while inserting RestrictInfo nodes at semantically appropriate locations