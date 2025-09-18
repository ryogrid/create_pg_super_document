# ListParsedLex

## Location
src/backend/tsearch/ts_parse.c: 35 - 39

## Overview
ListParsedLex is a structure that manages a doubly-ended linked list of ParsedLex nodes, providing efficient head and tail access for lexeme processing in PostgreSQL's text search system.

## Definition
```c
typedef struct ListParsedLex
{
    ParsedLex  *head;
    ParsedLex  *tail;
} ListParsedLex;
```

## Detailed Description
ListParsedLex is a container structure used in PostgreSQL's text search lexizer subsystem (src/backend/tsearch/ts_parse.c:35-39) to manage collections of ParsedLex nodes efficiently. It maintains pointers to both the first and last elements of a singly-linked list of ParsedLex structures, enabling O(1) operations for both adding elements to the tail and removing elements from the head.

This structure is essential for managing the flow of lexemes through the text search processing pipeline. It allows the lexizer to build up lists of parsed tokens and process them sequentially without needing to traverse the entire list to find insertion or removal points.

## Parameters / Member Variables
- `head`: Pointer to the first ParsedLex node in the list, or NULL if the list is empty
- `tail`: Pointer to the last ParsedLex node in the list, or NULL if the list is empty

## Dependencies
- Functions called/Symbols referenced:
  - [ParsedLex](../P/ParsedLex.md) (the node structure that this list manages)
- Called from (representative examples):
  - LPLAddTail (adds ParsedLex nodes to the tail of the list)
  - LPLRemoveHead (removes ParsedLex nodes from the head of the list)

## Notes and Other Information
- Provides efficient queue-like operations (FIFO - First In, First Out) for lexeme processing
- Both head and tail pointers are NULL when the list is empty
- When the list contains only one element, both head and tail point to the same ParsedLex node
- The structure itself does not allocate memory for the ParsedLex nodes; it only manages pointers to them
- Essential for maintaining order of lexemes as they are processed through the text search pipeline
- Used in conjunction with utility functions LPLAddTail and LPLRemoveHead for list manipulation