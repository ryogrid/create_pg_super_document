# ParsedLex

## Location
[src/backend/tsearch/ts_parse.c:27-33](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_parse.c#L27-L33)

## Overview
ParsedLex is a structure used in PostgreSQL's text search lexizer subsystem to represent a parsed lexeme with its type, text content, and position in a linked list of lexemes.

## Definition


## Detailed Description
ParsedLex is a fundamental data structure in PostgreSQL's text search parsing system, specifically used in the lexizer subsystem (src/backend/tsearch/ts_parse.c:27-33). It represents a single parsed lexeme (word or token) that has been processed by the text search parser. The structure forms nodes in a singly-linked list, allowing for efficient sequential processing of multiple lexemes during text search operations.

This structure is used throughout the text search lexization process to store intermediate results as text is broken down into searchable tokens. Each ParsedLex node contains the lexeme's classification type, the actual text content, its length, and a pointer to the next lexeme in the sequence.

## Parameters / Member Variables
- : Integer indicating the lexeme type or category as determined by the parser
- : Pointer to the actual text content of the lexeme (lemma)
- : Length of the lemma text in characters
- : Pointer to the next ParsedLex structure in the linked list, or NULL if this is the last node

## Dependencies
- Functions called/Symbols referenced:
  - (This structure is primarily referenced by other symbols rather than calling functions)
- Called from (representative examples):
  - LPLAddTail (adds ParsedLex to ListParsedLex)
  - LPLRemoveHead (removes ParsedLex from ListParsedLex)
  - LexizeAddLemm (creates and populates ParsedLex structures)
  - [LexizeExec](../L/LexizeExec.md) (processes ParsedLex during lexization)
  - [addHLParsedLex](../a/addHLParsedLex.md) (handles ParsedLex in highlighting functionality)

## Notes and Other Information
- Part of PostgreSQL's full-text search infrastructure
- Used in conjunction with ListParsedLex structure for managing collections of lexemes
- The structure supports efficient insertion and removal operations through its linked list design
- Memory management for the lemm field and the structure itself is handled by the calling functions
- Critical component in the text search pipeline that transforms raw text into searchable tokens