# makeDependencyGraphWalker

## Location
[src/backend/parser/parse_cte.c:670-811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_cte.c#L670-L811)

## Overview
Tree walker function that detects cross-references and self-references between CTEs in a WITH RECURSIVE list by traversing parse tree nodes.

## Definition


## Detailed Description
This function implements a recursive tree walker that examines parse tree nodes to identify CTE references and build dependency relationships. It performs several key operations:

1. **RangeVar Processing**: When encountering unqualified table references, checks if they refer to CTEs in the current WITH clause, distinguishing between self-references and cross-references
2. **Inner WITH Handling**: Properly handles nested WITH clauses by checking if references are captured by inner scopes before considering outer scope CTEs  
3. **Statement Type Processing**: Handles various statement types (SELECT, INSERT, UPDATE, DELETE, MERGE) that may contain WITH clauses, delegating to WalkInnerWith for proper scoping
4. **Dependency Tracking**: Records cross-CTE dependencies in the CteState structure using bitmapsets and marks self-referential CTEs as recursive

The walker uses PostgreSQL's raw_expression_tree_walker infrastructure to traverse the entire parse tree while handling CTE-specific logic.

## Parameters / Member Variables
- : Parse tree node being examined for CTE references
- : CteState structure containing CTE items and dependency tracking information

## Dependencies
- Functions called/Symbols referenced:
  - [bms_add_member](../b/bms_add_member.md) - adds dependencies to bitmapsets tracking CTE relationships
  - [WalkInnerWith](../W/WalkInnerWith.md) - handles nested WITH clauses with proper scoping
  - raw_expression_tree_walker - PostgreSQL's tree walking infrastructure for recursive traversal
  - [makeDependencyGraphWalker](makeDependencyGraphWalker.md) - recursive call to itself via raw_expression_tree_walker
- Called from (representative examples):
  - [makeDependencyGraph](makeDependencyGraph.md) - [main](main.md) entry point for dependency analysis
  - [WalkInnerWith](../W/WalkInnerWith.md) - for processing nested WITH clauses
  - [makeDependencyGraphWalker](makeDependencyGraphWalker.md) - recursively calls itself through tree walker

## Notes and Other Information
- Returns false in most cases to continue tree walking, true would stop traversal
- The function is static and only used within parse_cte.c  
- Handles scoping correctly by checking inner WITH clauses before outer ones
- Marks CTEs as recursive when they reference themselves
- Uses PostgreSQL's bitmapset data structure for efficient dependency tracking
- Prevents raw_expression_tree_walker from recursing into WITH clauses automatically
- Essential for building the dependency graph needed for topological sorting of recursive CTEs