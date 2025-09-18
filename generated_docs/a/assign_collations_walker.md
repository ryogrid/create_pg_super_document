# assign_collations_walker

## Location
[src/backend/parser/parse_collate.c:255-779](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_collate.c#L255-L779)

## Overview
The core recursive function that traverses an expression tree to assign collation information to all nodes based on their types and child expressions.

## Definition


## Detailed Description
This function is the recursive workhorse of PostgreSQL's collation assignment system. It walks through every node in an expression tree, determining the appropriate collation for each node based on several factors:

1. **Node Type Analysis**: Different node types have different collation inheritance rules (COLLATE expressions, field selections, aggregates, etc.)
2. **Child Collation Merging**: Combines collation information from child nodes using 
3. **Type System Integration**: Uses the PostgreSQL type system to determine if a node's result type is collatable
4. **Conflict Detection**: Identifies and reports collation conflicts where incompatible collations would be required

The function handles special cases for complex node types like aggregates (calling specialized functions like ), CASE expressions, and row comparisons. For most nodes, it follows a standard pattern: recurse to children, determine the node's collation based on type and child collations, then merge the result into the parent context.

## Parameters / Member Variables
- : The current expression node being processed (can be NULL for empty subexpressions)
- : Collation context containing state information including parser state and accumulated collation information

## Dependencies
- Functions called/Symbols referenced:
  -  (for recursive traversal)
  -  (for combining collation states)
  -  (for normal aggregates)
  -  (for ordered set aggregates)  
  -  (for hypothetical aggregates)
  - , ,  (collation accessors)
  -  (type system integration)
- Called from (representative examples):
  -  (entry point for expression collation assignment)
  - Itself (recursive calls for tree traversal)
  - Various aggregate collation assignment functions

## Notes and Other Information
- The function uses a local context for each recursion level to track collation state independently
- Special handling exists for nodes that don't contribute to parent collation (like RowExpr, join nodes)
- Error reporting includes source location information to help users identify collation conflicts
- The function sets both result collation and input collation on nodes, as functions may need different collation information for their inputs vs outputs