# LPLRemoveHead

## Location
[src/backend/tsearch/ts_parse.c:86-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_parse.c#L86-L99)

## Overview
LPLRemoveHead removes and returns the first ParsedLex element from a ListParsedLex linked list, maintaining proper list structure during dequeue operations.

## Definition
```c
static ParsedLex *LPLRemoveHead(ListParsedLex *list)
```

## Detailed Description
LPLRemoveHead implements head removal for a singly-linked list of ParsedLex elements, functioning as a dequeue operation. The function safely handles both single-element and multi-element lists by properly updating the head pointer and detecting when the list becomes empty. When the list becomes empty after removal, it ensures the tail pointer is also set to NULL to maintain list consistency.

The function returns the removed element without deallocating it, allowing the caller to decide how to handle the memory. This design supports both immediate processing and transfer to other data structures.

## Parameters / Member Variables
- `list`: Pointer to ListParsedLex structure representing the linked list to modify

## Return Value
- Returns pointer to the removed ParsedLex element, or NULL if the list was empty

## Dependencies
- Functions called/Symbols referenced:
  - [ListParsedLex](ListParsedLex.md) (structure type)
  - [ParsedLex](../P/ParsedLex.md) (structure type)
- Called from (representative examples):
  - [RemoveHead](../R/RemoveHead.md)

## Notes and Other Information
- Static function with local scope to ts_parse.c
- Maintains O(1) head removal performance
- Properly handles empty list case by returning NULL
- Does not deallocate the removed element's memory
- Essential for FIFO queue operations in lexeme processing
- Ensures list consistency by updating tail pointer when list becomes empty