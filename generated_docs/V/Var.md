# Var

## Location
src/include/nodes/primnodes.h: 247 - 294

## Overview
The Var structure represents a variable in PostgreSQL's expression tree, typically referring to a column in a table or a derived value from a subquery.

## Definition


## Detailed Description
The Var structure is a fundamental expression node type in PostgreSQL's query processing system. It represents references to table columns, computed expressions, or values from outer query levels. Vars are created during query parsing and planning phases and are used throughout the execution pipeline to identify and access specific data values. The structure includes both semantic information (what the variable actually refers to) and syntactic information (how it appeared in the original query text).

## Parameters / Member Variables
- : Base expression node structure containing common expression properties
- : Index of this variable's relation in the range table, or special values like INNER_VAR/OUTER_VAR for join contexts
- : Attribute number of this variable, or zero for whole-row references (all attributes)
- : PostgreSQL type system OID for the data type of this variable
- : Type modifier value from pg_attribute, providing additional type information
- : OID of the collation for this variable, or InvalidOid if no collation applies
- : Bitmap set of outer join relation indexes that can potentially set this variable's value to NULL
- : Nesting level for subquery variables (0 for current level, >0 for outer query levels)
- : Syntactic relation index as it appeared in the original query (0 if unknown)
- : Syntactic attribute number as it appeared in the original query
- : Token location in the original query text, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
- Called from (representative examples):
  - Used throughout the PostgreSQL query planning and execution system
  - Referenced in expression evaluation, optimization, and execution contexts

## Notes and Other Information
- The structure includes pg_node_attr annotations that control behavior during query jumbling and equality comparisons
- Syntactic fields (varnosyn/varattnosyn) are ignored during equality checks since semantic equivalence is determined by varno/varattno
- The varnullingrels field supports PostgreSQL's handling of outer joins and NULL value propagation
- This is a core data structure in PostgreSQL's expression system and appears extensively throughout the codebase