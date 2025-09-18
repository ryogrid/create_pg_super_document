# Expr

## Location
[src/include/nodes/primnodes.h:187-192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L187-L192)

## Overview
Expr is the generic superclass for all executable expression nodes in PostgreSQL's expression tree system, serving as the base type for expression node inheritance.

## Definition


## Detailed Description
Expr represents the abstract base class for PostgreSQL's executable expression node hierarchy. It serves as a documentation and type safety mechanism, establishing that all executable expression nodes should derive from Expr by having it as their first field. This creates a consistent inheritance pattern throughout the expression system.

The structure is marked with pg_node_attr(abstract), indicating it is an abstract base class that should not be instantiated directly. Instead, it provides a common interface for all expression node types used in executable expression trees, enabling polymorphic handling of different expression types.

Since Expr contains only the NodeTag field, it is primarily a formality for documentation and type system consistency, but it provides an important organizational structure for the complex expression hierarchy in PostgreSQL.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL's node system type identification, inherited by all expression node types

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (node system type identification)
  - pg_node_attr (node attribute system)

- Called from (representative examples):
  - This is an abstract base class, so it is not directly referenced but inherited by numerous expression node types such as:
    - Var (variable references)
    - Const (constant values)
    - OpExpr (operator expressions)
    - FuncExpr (function calls)
    - BoolExpr (boolean expressions)
    - And many other expression node types

## Notes and Other Information
- Abstract base class for PostgreSQL's expression node hierarchy
- Marked as abstract with pg_node_attr(abstract)
- All executable expression nodes should inherit from Expr
- Provides documentation and type consistency for expression trees
- Enables polymorphic handling of different expression types
- Critical component of PostgreSQL's expression evaluation system
- Works in conjunction with ExprState nodes in execnodes.h for execution
- Forms the foundation of PostgreSQL's expression type system
- Essential for expression tree traversal and manipulation functions
- Supports the node system's inheritance and type checking mechanisms