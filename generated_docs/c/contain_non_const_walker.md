# contain_non_const_walker

## Location
[src/backend/optimizer/util/clauses.c:3736-3751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L3736-L3751)

## Overview
A tree walker function that checks for the presence of non-constant nodes in expression trees, used to optimize constant expression evaluation by enabling early termination.

## Definition


## Detailed Description
This function serves as a subroutine for  to efficiently detect non-constant nodes within expression trees. Its primary purpose is performance optimization - by enabling immediate recursion abort upon finding any non-Const node, it prevents  from taking O(N^2) time on non-simplifiable trees.

The function implements a selective traversal strategy: it continues recursion only for List nodes (since  sometimes invokes the walker function directly on List subtrees) and Const nodes, but immediately returns true (indicating presence of non-const content) for any other node type.

## Parameters / Member Variables
- : The current node being examined in the expression tree traversal
- : Context parameter passed through the tree walking mechanism (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  -  - Used to recursively traverse List nodes
  -  - Recursive self-reference for List traversal
- Called from (representative examples):
  -  - Used to check if function arguments are all constants
  -  - Used in parallel query hazard assessment

## Notes and Other Information
- This is a static function, limiting its scope to the clauses.c file
- Critical for performance optimization in constant expression evaluation
- Uses the IsA() macro to perform type checking on nodes
- Returns false for NULL nodes and Const nodes (indicating no non-const content found)
- Returns true immediately for any non-Const, non-List node (indicating non-const content found)
- The early termination strategy is essential for maintaining linear time complexity in expression evaluation