# maketree

## Location
src/backend/utils/adt/tsquery_cleanup.c: 33 - 58

## Overview
The  function constructs a binary tree representation from a flat array representation of a TSQuery expression in PostgreSQL's text search functionality.

## Definition


## Detailed Description
The  function is a recursive function that transforms a linearized query structure (QueryItem array) into a binary tree of NODE structures. This conversion is essential for text search query processing, as it allows the system to work with a hierarchical tree representation that mirrors the logical structure of the query operators and operands.

The function performs recursive descent parsing, where each QueryItem in the input array is converted to a NODE. For operator items (QI_OPR), the function recursively constructs the right subtree and, if the operator is not NOT, also constructs the left subtree. The positioning within the array is calculated using the operator's left field to determine where the left operand begins.

The function includes stack overflow protection via  to prevent infinite recursion in malformed queries.

## Parameters / Member Variables
- : Pointer to a QueryItem in the flat array representation of the query, serving as the root of the subtree to be constructed

## Dependencies
- Functions called/Symbols referenced:
  - palloc (memory allocation)
  - check_stack_depth (stack overflow protection)
  - maketree (recursive self-call)
- Called from (representative examples):
  - clean_NOT
  - cleanup_tsquery_stopwords

## Notes and Other Information
- This function is part of PostgreSQL's text search query cleanup and optimization system
- The function uses recursive descent parsing to build the tree structure
- Memory for each NODE is allocated using PostgreSQL's memory management system (palloc)
- The function handles different query item types, with special logic for operators vs. operands
- Stack depth checking prevents potential denial-of-service attacks through deeply nested queries