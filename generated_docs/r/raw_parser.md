# raw_parser

## Location
[src/backend/parser/parser.c:42-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parser.c#L42-L110)

## Overview
The primary entry point for PostgreSQL's SQL parser that performs lexical and grammatical analysis on query strings, returning a list of raw (unanalyzed) parse trees.

## Definition

```c
enum */
		static const int mode_token[] = {
			[RAW_PARSE_DEFAULT] = 0,
			[RAW_PARSE_TYPE_NAME] = MODE_TYPE_NAME,
			[RAW_PARSE_PLPGSQL_EXPR] = MODE_PLPGSQL_EXPR,
			[RAW_PARSE_PLPGSQL_ASSIGN1] = MODE_PLPGSQL_ASSIGN1,
			[RAW_PARSE_PLPGSQL_ASSIGN2] = MODE_PLPGSQL_ASSIGN2,
			[RAW_PARSE_PLPGSQL_ASSIGN3] = MODE_PLPGSQL_ASSIGN3,
		};
```
## Detailed Description
The raw_parser function serves as the main interface to PostgreSQL's parsing subsystem. It takes a SQL query string and performs both lexical scanning (tokenization) and grammatical parsing to produce an Abstract Syntax Tree (AST). The function supports different parsing modes to handle various contexts like type names, PL/pgSQL expressions, and assignments.

The parsing process involves:
1. Initializing the flex scanner with the input string
2. Setting up mode-specific lookahead tokens for different parsing contexts
3. Initializing the bison parser
4. Executing the parse operation
5. Cleaning up scanner resources
6. Returning the resulting parse tree list

## Parameters / Member Variables
- : The input SQL query string to be parsed
- : The parsing mode (RawParseMode enum) that determines the parsing context and behavior

## Dependencies
- Functions called/Symbols referenced:
  - scanner_init
  - parser_init
  - base_yyparse
  - scanner_finish
  - RawParseMode (enum)
  - core_yyscan_t (type)
  - [base_yy_extra_type](../b/base_yy_extra_type.md) (type)
- Called from (representative examples):
  - [ATPostAlterTypeParse](../A/ATPostAlterTypeParse.md)
  - [_SPI_prepare_plan](../S/_SPI_prepare_plan.md)
  - [_SPI_prepare_oneshot_plan](../S/_SPI_prepare_oneshot_plan.md)
  - [typeStringToTypeName](../t/typeStringToTypeName.md)
  - [pg_parse_query](../p/pg_parse_query.md)

## Notes and Other Information
- Returns NIL on parsing errors
- Supports multiple parsing modes including default SQL, type names, and PL/pgSQL expressions
- The function handles memory management by cleaning up scanner resources regardless of parse success or failure
- Mode-specific lookahead tokens are used to provide context to the parser for specialized parsing scenarios

## Simplified Source

```c
// Simplified version of raw_parser
List *raw_parser(const char *str, RawParseMode mode) {
    core_yyscan_t yyscanner;
    base_yy_extra_type yyextra;
    int yyresult;

    // Initialize the flex scanner with the input string
    yyscanner = scanner_init(str, &yyextra.core_yy_extra,
                           &ScanKeywords, ScanKeywordTokens);

    // Set up mode-specific lookahead token if needed
    if (mode == RAW_PARSE_DEFAULT) {
        yyextra.have_lookahead = false;
    } else {
        // Map parsing modes to their corresponding tokens
        static const int mode_token[] = {
            [RAW_PARSE_DEFAULT] = 0,
            [RAW_PARSE_TYPE_NAME] = MODE_TYPE_NAME,
            [RAW_PARSE_PLPGSQL_EXPR] = MODE_PLPGSQL_EXPR,
            [RAW_PARSE_PLPGSQL_ASSIGN1] = MODE_PLPGSQL_ASSIGN1,
            [RAW_PARSE_PLPGSQL_ASSIGN2] = MODE_PLPGSQL_ASSIGN2,
            [RAW_PARSE_PLPGSQL_ASSIGN3] = MODE_PLPGSQL_ASSIGN3,
        };

        yyextra.have_lookahead = true;
        yyextra.lookahead_token = mode_token[mode];
        yyextra.lookahead_yylloc = 0;
        yyextra.lookahead_end = NULL;
    }

    // Initialize the bison parser
    parser_init(&yyextra);

    // Execute the parsing
    yyresult = base_yyparse(yyscanner);

    // Clean up scanner resources
    scanner_finish(yyscanner);

    // Return parse tree or NIL on error
    if (yyresult)
        return NIL;

    return yyextra.parsetree;
}
```

Key simplifications made:
- Added clear comments explaining each major step
- Preserved all essential logic and error handling
- Maintained the mode token mapping structure
- Kept memory cleanup and error handling intact
- Simplified conditional structure while preserving functionality