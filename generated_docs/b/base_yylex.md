# base_yylex

## Location
src/backend/parser/parser.c: 111 - 327

## Overview
An intermediate lexical analyzer filter between the parser and core lexer that handles multi-token lookahead requirements and converts Unicode identifier/string constants to standard tokens.

## Definition
```c
int base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner)
```

## Detailed Description
The base_yylex function serves as a filtering layer between PostgreSQL's grammar parser and the core lexical scanner (core_yylex). This design addresses two main challenges:

1. **Multi-token lookahead**: Some SQL grammar constructs require more than one token of lookahead to disambiguate. The filter reduces these to single-token lookahead by replacing tokens contextually, maintaining the grammar as LALR(1).

2. **Unicode token processing**: It converts UIDENT and USCONST sequences (with potential UESCAPE clauses) into plain IDENT and SCONST tokens, handling Unicode escape sequences and character validation.

The function implements a sophisticated lookahead mechanism for specific keywords (FORMAT, NOT, NULLS_P, WITH, WITHOUT) that require contextual analysis to determine their grammatical role. When these tokens are encountered, it examines the following token to decide whether to use a lookahead-aware version of the token.

For Unicode tokens, it processes escape sequences using either a specified escape character (from UESCAPE clause) or the default backslash, validates the escape character, and performs appropriate string conversion.

## Parameters / Member Variables
- `lvalp`: Pointer to the semantic value (YYSTYPE) to be filled with token data
- `llocp`: Pointer to location tracking information (YYLTYPE) for error reporting
- `yyscanner`: The scanner state object containing lexer context and buffers

## Dependencies
- Functions called/Symbols referenced:
  - pg_yyget_extra
  - core_yylex
  - [check_uescapechar](../c/check_uescapechar.md)
  - [str_udeescape](../s/str_udeescape.md)  
  - [truncate_identifier](../t/truncate_identifier.md)
  - scanner_yyerror
- Called from (representative examples):
  - [filtered_base_yylex](../f/filtered_base_yylex.md) (in ECPG preprocessor)

## Notes and Other Information
- Implements careful location tracking for error reporting, ensuring errors point to the correct token position
- Handles memory management for lookahead tokens and character buffer manipulation
- The filter approach is more efficient than trying to recognize multi-word tokens directly in the scanner
- Critical for maintaining PostgreSQL's grammar as LALR(1) while supporting complex SQL constructs
- Unicode processing includes validation of escape characters and proper identifier truncation according to PostgreSQL rules