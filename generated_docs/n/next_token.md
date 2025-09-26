# next_token

## Location
[src/backend/libpq/hba.c:185-256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L185-L256)

## Overview
A tokenizer function that extracts the next token from a configuration line, handling quoted strings, comments, and delimiters for HBA (Host-Based Authentication) file parsing.

## Definition
```c
static bool next_token(char **lineptr, StringInfo buf, bool *initial_quote, bool *terminating_comma)
```

## Detailed Description
The next_token function is a sophisticated string tokenizer specifically designed for parsing PostgreSQL's HBA (Host-Based Authentication) configuration files. It extracts individual tokens from a line of text, handling various parsing rules:

- Tokens are delimited by whitespace (space, tab, carriage return), commas, beginning of line, or end of line
- Double quotes can be used to include whitespace and special characters within tokens
- Double-quotes within quoted strings are escaped by writing two consecutive double-quotes
- Comments starting with '#' (when not inside quotes) cause the remainder of the line to be ignored
- The function tracks whether the token started with a quote and whether it was terminated by a comma

The function modifies the input line pointer to advance past the processed token and returns the extracted token in a StringInfo buffer.

## Parameters / Member Variables
- `lineptr`: Pointer to a char pointer that points to the current position in the line being parsed; advanced past the token
- `buf`: StringInfo buffer where the extracted token is stored (previous contents are replaced)
- `initial_quote`: Output parameter set to true if the token started with a double quote
- `terminating_comma`: Output parameter set to true if the token was terminated by a comma

## Dependencies
- Functions called/Symbols referenced:
  - [resetStringInfo](../r/resetStringInfo.md) (clears the output buffer)
  - [pg_isblank](../p/pg_isblank.md) (tests for whitespace characters)
  - [appendStringInfoChar](../a/appendStringInfoChar.md) (appends characters to the buffer)
- Called from (representative examples):
  - [next_field_expand](next_field_expand.md) (in src/backend/libpq/hba.c)
  - [base_yylex](../b/base_yylex.md) (in src/backend/parser/parser.c)
  - [filtered_base_yylex](../f/filtered_base_yylex.md) (in src/interfaces/ecpg/preproc/parser.c)

## Notes and Other Information
- This is a static function, only visible within the hba.c file
- The function handles SQL-style double-quote escaping (two quotes represent one quote)
- The initial_quote parameter is used to distinguish between @filename and @"filename" for file inclusion
- Comments are completely skipped - once a '#' is encountered outside quotes, the rest of the line is ignored
- The function returns true if a token was found (either quoted or non-empty), false if no more tokens exist
- Used primarily in PostgreSQL's authentication subsystem for parsing configuration files