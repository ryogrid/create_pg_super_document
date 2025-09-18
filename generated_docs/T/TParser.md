# TParser

## Location
src/backend/tsearch/wparser_def.c: 241 - 264

## Overview
TParser is the main structure that encapsulates the complete state and context of PostgreSQL's text search parser, managing input text processing, character encoding handling, and token generation.

## Definition


## Detailed Description
TParser serves as the central data structure for PostgreSQL's text search word parser. It manages the complete parsing context including the input text in multiple character encodings, the current parser state stack, and output token information. The parser supports both single-byte and wide character processing to handle various character encodings correctly.

The structure maintains multiple representations of the input text (multibyte, wide character, and PostgreSQL's internal wide character format) to optimize processing for different character encoding scenarios. It uses a state stack (via TParserPosition) to enable complex parsing scenarios with backtracking capabilities.

## Parameters / Member Variables
- : Pointer to the input text in multibyte character encoding
- : Length of the multibyte input string
- : Wide character representation of the input string (wchar_t format)
- : PostgreSQL's internal wide character representation for C-locale processing
- : Boolean flag indicating whether wide character processing is being used
- : Maximum length of a character in the current encoding
- : Pointer to the current parser position/state stack (TParserPosition)
- : Boolean flag indicating whether certain characters should be ignored
- : Boolean flag used for URL/hostname parsing logic
- : Current character being processed (single-byte representation)
- : Output buffer containing the current token being generated
- : Length of the current output token in bytes
- : Length of the current output token in characters
- : Token type identifier for the current token

## Dependencies
- Functions called/Symbols referenced:
  - [TParserPosition](TParserPosition.md)
- Called from (representative examples):
  - TParserInit
  - TParserCopyInit
  - TParserClose
  - TParserCopyClose
  - [TParserGet](TParserGet.md)
  - [prsd_nexttoken](../p/prsd_nexttoken.md)
  - [prsd_end](../p/prsd_end.md)
  - Various parser utility functions (p_iswhat, p_isEOF, etc.)

## Notes and Other Information
This structure is the heart of PostgreSQL's text search tokenization system. The multiple character encoding representations allow the parser to efficiently handle different locales and character sets. The state stack mechanism (via the state field) enables sophisticated parsing logic that can handle complex text patterns requiring lookahead and backtracking. The parser is designed to be robust across different character encodings and locales, which is crucial for PostgreSQL's international text search capabilities.