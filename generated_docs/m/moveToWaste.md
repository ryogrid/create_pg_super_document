# moveToWaste

## Location
[src/backend/tsearch/ts_parse.c:142-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_parse.c#L142-L157)

## Overview
Moves lexemes from the work queue to the waste list, stopping at a specified lexeme and updating the current processing position.

## Definition
```c
static void moveToWaste(LexizeData *ld, ParsedLex *stop)
```

## Detailed Description
The moveToWaste function transfers lexemes from the towork queue to the waste list within a LexizeData structure. It processes lexemes from the head of the towork queue, removing them one by one using RemoveHead(), until it encounters the specified stop lexeme or the queue becomes empty.

When the stop lexeme is found, the function:
1. Sets the curSub pointer to the next lexeme after the stop lexeme
2. Stops processing (sets go = false)
3. Removes the stop lexeme itself via RemoveHead()

This function is typically used during text search parsing to clean up processed lexemes while maintaining proper queue state for continued processing.

## Parameters / Member Variables
- `ld`: Pointer to LexizeData structure containing the towork queue and waste list
- `stop`: Pointer to ParsedLex that serves as the stopping point for the move operation

## Dependencies
- Functions called/Symbols referenced:
  - RemoveHead (removes and moves lexemes from towork to waste)
- Called from (representative examples):
  - [LexizeExec](../L/LexizeExec.md) (at lines 319, 324 in ts_parse.c)

## Notes and Other Information
- This is a static function, only accessible within the ts_parse.c compilation unit
- The function ensures proper queue management during lexeme processing
- The curSub pointer is updated to maintain continuity in processing after the move operation
- RemoveHead() is responsible for the actual transfer mechanism from towork to waste