# setNewTmpRes

## Location
src/backend/tsearch/ts_parse.c: 158 - 172

## Overview
Sets new temporary results in the LexizeData structure while properly cleaning up any previously stored temporary results to prevent memory leaks.

## Definition
```c
static void setNewTmpRes(LexizeData *ld, ParsedLex *lex, TSLexeme *res)
```

## Detailed Description
The setNewTmpRes function manages temporary lexeme results during text search parsing operations. It performs two main tasks:

1. **Cleanup of existing results**: If there are existing temporary results (ld->tmpRes is not NULL), it iterates through the TSLexeme array and frees each lexeme string, then frees the array itself
2. **Assignment of new results**: Sets the new TSLexeme array as the current temporary result and updates the lastRes pointer to track which ParsedLex generated these results

The function ensures proper memory management by cleaning up old results before assigning new ones, preventing memory leaks during the lexing process.

## Parameters / Member Variables
- `ld`: Pointer to LexizeData structure that stores the temporary results
- `lex`: Pointer to ParsedLex that generated the new results (stored in lastRes)
- `res`: Array of TSLexeme structures representing the new temporary results

## Dependencies
- Functions called/Symbols referenced:
  - pfree (PostgreSQL memory deallocation function)
- Called from (representative examples):
  - LexizeExec (at lines 225, 306 in ts_parse.c)

## Notes and Other Information
- This is a static function, only accessible within the ts_parse.c compilation unit
- The function assumes TSLexeme arrays are NULL-terminated (loops until ptr->lexeme is NULL)
- Proper cleanup is essential as lexeme processing can generate multiple temporary results
- The lastRes field helps track which parsed lexeme corresponds to the current temporary results
- Memory management follows PostgreSQL conventions using pfree() for deallocation