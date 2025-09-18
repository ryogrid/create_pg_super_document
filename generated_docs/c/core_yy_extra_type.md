# core_yy_extra_type

## Location
src/include/parser/scanner.h: 66 - 116

## Overview
The core scanner's private state structure that contains all necessary data for lexical analysis in PostgreSQL's SQL parser, designed to be embedded as the first component of larger scanner state structures.

## Definition


## Detailed Description
This structure serves as the YY_EXTRA data that a flex scanner allows to be passed around during lexical analysis. It contains all the private state needed by PostgreSQL's core scanner. The structure is designed to be extensible - the actual yy_extra struct in calling parsers may be larger and have this as its first component, allowing parser-specific fields to be added while maintaining compatibility with the core scanner functionality.

The structure manages various aspects of SQL tokenization including buffer management, keyword recognition, string literal processing, comment handling, and Unicode escape sequences. It also maintains scanner settings that can be initialized from GUC variables and modified by callers to control scanner behavior.

## Parameters / Member Variables
- : The string buffer that the scanner is physically scanning, used for computing token offsets
- : Length of the scan buffer
- : Pointer to the keyword list used for token recognition
- : Associated grammar token codes for keywords
- : Scanner setting for backslash quote handling (from GUC variables)
- : Setting to control escape string warnings
- : Setting for standard conforming string behavior
- : Expandable buffer for accumulating literal values during multi-rule parsing
- : Current actual length of the literal string
- : Current allocated size of the literal buffer
- : Start condition before encountering end quote
- : Nesting depth in slash-star comments
- : Current dollar-quote start string (e.g., $foo$)
- : One-element stack for PUSH_YYLLOC() macro operations
- : First part of UTF16 surrogate pair for Unicode escapes
- : State variable for literal-lexing warnings
- : State variable tracking non-ASCII characters in literals

## Dependencies
- Functions called/Symbols referenced:
  - ScanKeywordList
  - YYLTYPE
- Called from (representative examples):
  - base_yy_extra_type

## Notes and Other Information
This structure is fundamental to PostgreSQL's lexical analysis system and must be carefully maintained to ensure proper SQL parsing. The literalbuf mechanism allows for efficient handling of complex string literals that require multiple lexer rules. The dollar-quote support enables PostgreSQL's extended string quoting functionality. Scanner settings can be modified after initialization to override GUC-based defaults when needed.