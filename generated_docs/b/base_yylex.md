# base_yylex

## Location
[src/backend/parser/parser.c:111-327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parser.c#L111-L327)

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

## Simplified Source

```c
int base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner) {
    base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);
    int cur_token, next_token, cur_token_length;
    YYLTYPE cur_yylloc;

    // Get current token (either cached lookahead or new from core lexer)
    if (yyextra->have_lookahead) {
        cur_token = yyextra->lookahead_token;
        lvalp->core_yystype = yyextra->lookahead_yylval;
        *llocp = yyextra->lookahead_yylloc;
        yyextra->have_lookahead = false;
    } else {
        cur_token = core_yylex(&(lvalp->core_yystype), llocp, yyscanner);
    }

    // Check if token requires lookahead processing
    switch (cur_token) {
        case FORMAT: cur_token_length = 6; break;
        case NOT: cur_token_length = 3; break;
        case NULLS_P: cur_token_length = 5; break;
        case WITH: cur_token_length = 4; break;
        case WITHOUT: cur_token_length = 7; break;
        case UIDENT:
        case USCONST:
            cur_token_length = strlen(yyextra->core_yy_extra.scanbuf + *llocp);
            break;
        default:
            return cur_token;  // No lookahead needed
    }

    // Set up for lookahead processing
    yyextra->lookahead_end = yyextra->core_yy_extra.scanbuf + *llocp + cur_token_length;
    cur_yylloc = *llocp;

    // Get next token for lookahead
    next_token = core_yylex(&(yyextra->lookahead_yylval), llocp, yyscanner);
    yyextra->lookahead_token = next_token;
    yyextra->lookahead_yylloc = *llocp;
    *llocp = cur_yylloc;

    // Restore token state
    yyextra->lookahead_hold_char = *(yyextra->lookahead_end);
    *(yyextra->lookahead_end) = '\0';
    yyextra->have_lookahead = true;

    // Apply lookahead-based token replacement
    switch (cur_token) {
        case FORMAT:
            if (next_token == JSON) cur_token = FORMAT_LA;
            break;
        case NOT:
            if (next_token == BETWEEN || next_token == IN_P ||
                next_token == LIKE || next_token == ILIKE || next_token == SIMILAR)
                cur_token = NOT_LA;
            break;
        case NULLS_P:
            if (next_token == FIRST_P || next_token == LAST_P)
                cur_token = NULLS_LA;
            break;
        case WITH:
            if (next_token == TIME || next_token == ORDINALITY)
                cur_token = WITH_LA;
            break;
        case WITHOUT:
            if (next_token == TIME) cur_token = WITHOUT_LA;
            break;
        case UIDENT:
        case USCONST:
            // Process Unicode escape sequences
            if (next_token == UESCAPE) {
                // Handle UESCAPE followed by string literal
                cur_yylloc = *llocp;
                *(yyextra->lookahead_end) = yyextra->lookahead_hold_char;
                next_token = core_yylex(&(yyextra->lookahead_yylval), llocp, yyscanner);

                if (next_token != SCONST)
                    scanner_yyerror("UESCAPE must be followed by a simple string literal", yyscanner);

                const char *escstr = yyextra->lookahead_yylval.str;
                if (strlen(escstr) != 1 || !check_uescapechar(escstr[0]))
                    scanner_yyerror("invalid Unicode escape character", yyscanner);

                *llocp = cur_yylloc;
                lvalp->core_yystype.str = str_udeescape(lvalp->core_yystype.str, escstr[0], *llocp, yyscanner);
                yyextra->have_lookahead = false;
            } else {
                // Use default escape character
                lvalp->core_yystype.str = str_udeescape(lvalp->core_yystype.str, '\\', *llocp, yyscanner);
            }

            // Convert to appropriate token type
            if (cur_token == UIDENT) {
                truncate_identifier(lvalp->core_yystype.str, strlen(lvalp->core_yystype.str), true);
                cur_token = IDENT;
            } else {
                cur_token = SCONST;
            }
            break;
    }

    return cur_token;
}
```