# StatEntry

## Location
src/backend/utils/adt/tsvector_op.c: 45 - 54

## Overview
StatEntry is a structure used in PostgreSQL's text search functionality to maintain statistical information about lexemes in TSVector objects, organized as nodes in a binary search tree for efficient lexeme frequency tracking and retrieval.

## Definition


## Detailed Description
StatEntry serves as a binary search tree node structure for collecting and organizing statistical data about lexemes (words) found in TSVector objects during text search operations. Each StatEntry represents a unique lexeme along with its frequency statistics. The structure is designed to support efficient insertion, searching, and in-order traversal of lexemes while maintaining statistical counts.

The binary tree organization allows for efficient alphabetical ordering of lexemes, with the tree traversal supporting the  function that returns lexeme frequency statistics. The  field serves a dual purpose: it stores the document frequency count and is also used as a traversal flag during tree walking operations (set to 0 when a node has been visited during traversal).

The structure uses a flexible array member for the lexeme text, allowing for variable-length lexeme storage without additional memory allocations. This design optimizes memory usage by storing the lexeme text directly within the StatEntry structure.

## Parameters / Member Variables
- : Document frequency count for this lexeme; also used as a visited flag (0 = already visited) during tree traversal operations
- : Number of entries/occurrences of this lexeme in the analyzed text corpus
- : Pointer to the left child node in the binary search tree (lexemes that come before this one alphabetically)
- : Pointer to the right child node in the binary search tree (lexemes that come after this one alphabetically) 
- : Length in bytes of the lexeme string stored in the  field
- : Variable-length array containing the actual lexeme text (word/token)

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for variable-length arrays)
- Called from (representative examples):
  - insertStatEntry (creates and populates new StatEntry nodes)
  - walkStatEntryTree (traverses the binary tree of StatEntry nodes)
  - ts_setup_firstcall (initializes traversal stack for StatEntry tree)
  - ts_process_call (processes StatEntry nodes during result generation)

## Notes and Other Information
- The binary tree structure enables efficient alphabetical ordering and retrieval of lexeme statistics
- STATENTRYHDRSZ macro is defined as  to calculate the header size excluding the flexible array member
- Used exclusively within the tsvector_op.c file for implementing the  SQL function
- Memory allocation includes space for both the fixed structure and the variable-length lexeme text
- The tree traversal algorithm uses a stack-based approach to support resumable iteration in PostgreSQL's set-returning function framework
- The  field's dual purpose (count and flag) is an optimization to avoid additional traversal state tracking