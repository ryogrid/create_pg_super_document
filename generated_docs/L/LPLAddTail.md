# LPLAddTail

## Location
src/backend/tsearch/ts_parse.c: 73 - 85

## Overview
LPLAddTail adds a ParsedLex element to the tail (end) of a ListParsedLex linked list, maintaining proper list structure for lexeme processing.

## Definition
```c
static void LPLAddTail(ListParsedLex *list, ParsedLex *newpl)
```

## Detailed Description
LPLAddTail is a static utility function that implements tail insertion for a singly-linked list of ParsedLex elements. The function handles both empty and non-empty list cases properly, ensuring the list's head and tail pointers remain consistent. When the list is empty, the new element becomes both head and tail. For non-empty lists, the function links the new element after the current tail and updates the tail pointer.

This function is essential for building ordered sequences of parsed lexemes during text search operations, allowing efficient append operations while maintaining list integrity.

## Parameters / Member Variables
- `list`: Pointer to ListParsedLex structure representing the linked list to modify
- `newpl`: Pointer to ParsedLex element to add at the end of the list

## Dependencies
- Functions called/Symbols referenced:
  - ListParsedLex (structure type)
  - ParsedLex (structure type)
- Called from (representative examples):
  - LexizeAddLemm
  - RemoveHead

## Notes and Other Information
- Static function with local scope to ts_parse.c
- Maintains O(1) tail insertion performance by tracking tail pointer
- Properly handles empty list initialization
- Essential for lexeme queue management in text search processing
- Ensures null termination of the newly added element