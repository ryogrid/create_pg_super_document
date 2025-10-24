# LPLAddTail

## Location
[src/backend/tsearch/ts_parse.c:73-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_parse.c#L73-L85)

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
  - [ListParsedLex](ListParsedLex.md) (structure type)
  - [ParsedLex](../P/ParsedLex.md) (structure type)
- Called from (representative examples):
  - [LexizeAddLemm](LexizeAddLemm.md)
  - [RemoveHead](../R/RemoveHead.md)

## Notes and Other Information
- Static function with local scope to ts_parse.c
- Maintains O(1) tail insertion performance by tracking tail pointer
- Properly handles empty list initialization
- Essential for lexeme queue management in text search processing
- Ensures null termination of the newly added element

## Simplified Source

```c
static void
LPLAddTail(ListParsedLex *list, ParsedLex *newpl)
{
    // Add to end of non-empty list
    if (list->tail)
    {
        list->tail->next = newpl;
        list->tail = newpl;
    }
    else
    {
        // Initialize empty list
        list->head = list->tail = newpl;
    }

    // Ensure new element terminates the list
    newpl->next = NULL;
}
```