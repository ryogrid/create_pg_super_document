# MultiAssignRef

## Location
src/include/nodes/parsenodes.h: 532 - 538

## Overview
MultiAssignRef is a parse tree node used in UPDATE statements to represent individual column targets when assigning values from a row-valued expression using multi-column assignment syntax like SET (a,b,c) = row-valued-expression.

## Definition


## Detailed Description
MultiAssignRef nodes are generated during parsing of UPDATE statements that use multi-column assignment syntax. When PostgreSQL encounters an UPDATE with SET (a,b,c) = row-valued-expression, it creates separate ResTarget items for each target column (a, b, c). Each ResTarget's "val" tree contains a MultiAssignRef node numbered 1 through n, all linking to a common copy of the row-valued expression. This design ensures that the row-valued expression is processed only once during parse analysis (when handling the MultiAssignRef with colno=1), improving efficiency and maintaining consistency.

## Parameters / Member Variables
- : Standard NodeTag identifying this as a MultiAssignRef node
- : Pointer to the row-valued expression that provides the values for all columns in the assignment
- : The column number for this specific target (1-based indexing, ranges from 1 to n)
- : Total number of target columns in the multi-assignment construct

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited structure member)
  - Node (base type for source pointer)
- Called from (representative examples):
  - transformMultiAssignRef (src/backend/parser/parse_expr.c:1484)
  - transformExprRecurse (src/backend/parser/parse_expr.c:233)
  - raw_expression_tree_walker_impl (src/backend/nodes/nodeFuncs.c:4413)
  - exprLocation (src/backend/nodes/nodeFuncs.c:1687)

## Notes and Other Information
- MultiAssignRef is specifically designed for UPDATE statement processing and is part of PostgreSQL's parse tree structure
- The colno=1 MultiAssignRef is special as it triggers the actual processing of the shared row-valued expression
- This node type optimizes multi-column assignments by avoiding redundant processing of the source expression
- File location: src/include/nodes/parsenodes.h:532-538