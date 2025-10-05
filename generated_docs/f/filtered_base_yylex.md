# filtered_base_yylex

## Location
[src/interfaces/ecpg/preproc/parser.c:56-227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/parser.c#L56-L227)

## Overview
An intermediate lexical filter between the parser and base lexer that handles multi-token lookahead scenarios and converts Unicode identifiers/string constants to their standard forms.

## Definition
```c
int filtered_base_yylex(void)
```

## Detailed Description
This function serves as a crucial component in the ECPG preprocessor lexical analysis pipeline. It acts as an intermediate filter between the parser and the base lexer (base_yylex in scan.l) to address two main challenges:

1. **Multi-token lookahead reduction**: The standard SQL grammar sometimes requires more than one token lookahead, which would violate LALR(1) grammar requirements. This filter reduces these cases to one-token lookahead by replacing tokens based on context.

2. **Unicode token conversion**: Converts UIDENT and USCONST sequences (potentially with UESCAPE clauses) into plain IDENT and SCONST tokens for simpler grammar processing.

The function implements a lookahead mechanism using global variables to peek at the next token and make contextual decisions about token replacement. This approach is more efficient than trying to recognize multiword tokens directly in the scanner, as it avoids the complexity of handling comments between words and prevents the need for scanner backtracking.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [base_yylex](../b/base_yylex.md): The underlying lexer function that generates raw tokens
  - [check_uescapechar](../c/check_uescapechar.md): Validates Unicode escape characters
  - `mmerror`: Reports parsing errors
  - [psprintf](../p/psprintf.md): PostgreSQL string formatting function
  - `strlen`: Standard C string length function

- Called from (representative examples):
  - Parser functions in the ECPG preprocessor (indirectly through lexer interface)

## Notes and Other Information
- The function maintains lookahead state using global variables: `have_lookahead`, `lookahead_token`, `lookahead_yylval`, `lookahead_yylloc`, and `lookahead_yytext`
- Token replacement patterns include:
  - `FORMAT` → `FORMAT_LA` when followed by `JSON`
  - `NOT` → `NOT_LA` when followed by `BETWEEN`, `IN_P`, `LIKE`, `ILIKE`, or `SIMILAR`
  - `NULLS_P` → `NULLS_LA` when followed by `FIRST_P` or `LAST_P`
  - `WITH` → `WITH_LA` when followed by `TIME` or `ORDINALITY`
  - `WITHOUT` → `WITHOUT_LA` when followed by `TIME`
  - `UIDENT` → `IDENT` (after processing potential UESCAPE)
  - `USCONST` → `SCONST` (after processing potential UESCAPE)
- Unicode escape processing validates that the escape character is exactly 3 characters long (including quotes) and uses `check_uescapechar` for validation
- The filter is essential for maintaining ECPG preprocessors ability to handle complex SQL syntax while keeping the grammar manageable

## Simplified Source

```c
int filtered_base_yylex(void) {
    int cur_token, next_token;
    YYSTYPE cur_yylval;
    YYLTYPE cur_yylloc;
    char *cur_yytext;

    // Get next token from lookahead or base lexer
    if (have_lookahead) {
        cur_token = lookahead_token;
        base_yylval = lookahead_yylval;
        base_yylloc = lookahead_yylloc;
        base_yytext = lookahead_yytext;
        have_lookahead = false;
    } else {
        cur_token = base_yylex();
    }

    // Return immediately if no lookahead needed
    switch (cur_token) {
        case FORMAT:
        case NOT:
        case NULLS_P:
        case WITH:
        case WITHOUT:
        case UIDENT:
        case USCONST:
            break;
        default:
            return cur_token;
    }

    // Save current token state
    cur_yylval = base_yylval;
    cur_yylloc = base_yylloc;
    cur_yytext = base_yytext;

    // Get next token for lookahead
    next_token = base_yylex();
    lookahead_token = next_token;
    lookahead_yylval = base_yylval;
    lookahead_yylloc = base_yylloc;
    lookahead_yytext = base_yytext;

    // Restore current token state
    base_yylval = cur_yylval;
    base_yylloc = cur_yylloc;
    base_yytext = cur_yytext;
    have_lookahead = true;

    // Apply token replacements based on lookahead
    switch (cur_token) {
        case FORMAT:
            if (next_token == JSON)
                cur_token = FORMAT_LA;
            break;
        case NOT:
            if (next_token == BETWEEN || next_token == IN_P || next_token == LIKE ||
                next_token == ILIKE || next_token == SIMILAR)
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
            if (next_token == TIME)
                cur_token = WITHOUT_LA;
            break;
        case UIDENT:
        case USCONST:
            // Handle Unicode escape sequences
            if (next_token == UESCAPE) {
                // Process UESCAPE followed by string constant
                cur_yylval = base_yylval;
                cur_yylloc = base_yylloc;
                cur_yytext = base_yytext;

                next_token = base_yylex();
                if (next_token != SCONST)
                    mmerror(PARSE_ERROR, ET_ERROR, "UESCAPE must be followed by a simple string literal");

                // Validate and combine tokens
                const char *escstr = base_yylval.str;
                if (strlen(escstr) != 3 || !check_uescapechar(escstr[1]))
                    mmerror(PARSE_ERROR, ET_ERROR, "invalid Unicode escape character");

                base_yylval = cur_yylval;
                base_yylloc = cur_yylloc;
                base_yytext = cur_yytext;
                base_yylval.str = psprintf("%s UESCAPE %s", base_yylval.str, escstr);
                have_lookahead = false;
            }

            // Convert Unicode tokens to standard tokens
            if (cur_token == UIDENT)
                cur_token = IDENT;
            else if (cur_token == USCONST)
                cur_token = SCONST;
            break;
    }

    return cur_token;
}
```