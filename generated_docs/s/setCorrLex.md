# setCorrLex

## Location
src/backend/tsearch/ts_parse.c: 120 - 141

## Overview
Sets the corresponding lexeme for a text search parsing operation by managing the waste list of parsed lexemes, either returning it to the caller or cleaning it up.

## Definition


## Detailed Description
The setCorrLex function manages the waste list within the LexizeData structure during text search parsing operations. It serves a dual purpose based on whether a correspondLexem parameter is provided:

1. If correspondLexem is non-NULL, it transfers ownership of the waste list to the caller by setting *correspondLexem to point to the head of the waste list
2. If correspondLexem is NULL, it cleans up the waste list by iterating through all ParsedLex nodes and freeing their memory

After either operation, the function resets both the head and tail pointers of the waste list to NULL, effectively clearing the waste list from the LexizeData structure.

## Parameters / Member Variables
- : Pointer to LexizeData structure containing the waste list to be processed
- : Double pointer to ParsedLex; if non-NULL, receives the waste list; if NULL, triggers cleanup of the waste list

## Dependencies
- Functions called/Symbols referenced:
  - pfree (PostgreSQL memory deallocation function)
- Called from (representative examples):
  - LexizeExec (at lines 240, 332, 345 in ts_parse.c)

## Notes and Other Information
- This is a static function, only accessible within the ts_parse.c compilation unit
- The function implements a memory management pattern common in PostgreSQL where resources can either be transferred to a caller or automatically cleaned up
- The waste list appears to contain ParsedLex structures that are no longer needed during the parsing process
- Proper cleanup is essential to prevent memory leaks in text search operations