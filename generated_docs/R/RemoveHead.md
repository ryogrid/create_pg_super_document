# RemoveHead

## Location
[src/backend/tsearch/ts_parse.c:112-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_parse.c#L112-L119)

## Overview
RemoveHead removes the first element from the work queue and moves it to the waste queue, resetting dictionary processing position for lexeme management.

## Definition
```c
static void RemoveHead(LexizeData *ld)
```

## Detailed Description
RemoveHead is a static function that implements a transfer operation between two queues within the LexizeData structure. It removes the head element from the towork queue and immediately adds it to the tail of the waste queue, effectively moving processed or unwanted lexemes out of the active work flow. 

The function also resets the posDict counter to 0, indicating that dictionary processing should restart from the beginning for any remaining elements in the work queue. This is crucial for maintaining proper state when lexemes are rejected or need to be reprocessed with different dictionaries.

## Parameters / Member Variables
- `ld`: Pointer to LexizeData structure containing both work and waste queues

## Dependencies
- Functions called/Symbols referenced:
  - LexizeData (structure type)
  - LPLAddTail (list manipulation function)
  - LPLRemoveHead (list manipulation function)
- Called from (representative examples):
  - moveToWaste
  - LexizeExec (multiple locations)

## Notes and Other Information
- Static function with local scope to ts_parse.c
- Performs atomic transfer between work and waste queues
- Resets dictionary position counter for remaining elements
- Essential for lexeme lifecycle management in text search processing
- Maintains efficient queue operations while preserving processed elements for potential cleanup
- Part of the lexical analysis state machine in PostgreSQL's full-text search