# setCorrLex

## Location
[src/backend/tsearch/ts_parse.c:120-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_parse.c#L120-L141)

## Overview
Sets the corresponding lexeme for a text search parsing operation by managing the waste list of parsed lexemes, either returning it to the caller or cleaning it up.

## Definition

```c
static void
setCorrLex(LexizeData *ld, ParsedLex **correspondLexem)
```
## Detailed Description
The setCorrLex function manages the waste list within the LexizeData structure during text search parsing operations. It serves a dual purpose based on whether a correspondLexem parameter is provided:

1. If correspondLexem is non-NULL, it transfers ownership of the waste list to the caller by setting *correspondLexem to point to the head of the waste list
2. If correspondLexem is NULL, it cleans up the waste list by iterating through all ParsedLex nodes and freeing their memory

After either operation, the function resets both the head and tail pointers of the waste list to NULL, effectively clearing the waste list from the LexizeData structure.

## Parameters / Member Variables
- `*ld`: Pointer to LexizeData structure containing the waste list to be processed
- `**correspondLexem`: Double pointer to ParsedLex; if non-NULL, receives the waste list; if NULL, triggers cleanup of the waste list
## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
- Called from (representative examples):
  - [LexizeExec](../L/LexizeExec.md) (at lines 240, 332, 345 in ts_parse.c)

## Notes and Other Information
- This is a static function, only accessible within the ts_parse.c compilation unit
- The function implements a memory management pattern common in PostgreSQL where resources can either be transferred to a caller or automatically cleaned up
- The waste list appears to contain ParsedLex structures that are no longer needed during the parsing process
- Proper cleanup is essential to prevent memory leaks in text search operations

## Simplified Source

```c
static void setCorrLex(LexizeData *ld, ParsedLex **correspondLexem) {
    if (correspondLexem) {
        // Transfer waste list to caller
        *correspondLexem = ld->waste.head;
    } else {
        // Clean up waste list by freeing all nodes
        ParsedLex *ptr = ld->waste.head;
        while (ptr) {
            ParsedLex *tmp = ptr->next;
            pfree(ptr);
            ptr = tmp;
        }
    }

    // Clear waste list pointers
    ld->waste.head = ld->waste.tail = NULL;
}
```