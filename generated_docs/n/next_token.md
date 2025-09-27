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

## Simplified Source

```c
// Simplified version of next_token
static bool next_token(char **lineptr, StringInfo buf,
                      bool *initial_quote, bool *terminating_comma) {
    int c;
    bool in_quote = false;
    bool was_quote = false;
    bool saw_quote = false;

    // Initialize output parameters
    resetStringInfo(buf);
    *initial_quote = false;
    *terminating_comma = false;

    // Skip whitespace and commas before token
    while ((c = (*(*lineptr)++)) != '\0' && (pg_isblank(c) || c == ','))
        ;

    // Extract token until EOL, unquoted comma, or unquoted whitespace
    while (c != '\0' && (!pg_isblank(c) || in_quote)) {
        // Skip comments to end of line
        if (c == '#' && !in_quote) {
            while ((c = (*(*lineptr)++)) != '\0')
                ;
            break;
        }

        // Stop at unquoted comma (don't include it in token)
        if (c == ',' && !in_quote) {
            *terminating_comma = true;
            break;
        }

        // Add character to token (unless it's a quote being processed)
        if (c != '"' || was_quote)
            appendStringInfoChar(buf, c);

        // Handle double-quote escaping (two quotes = one literal quote)
        if (in_quote && c == '"')
            was_quote = !was_quote;
        else
            was_quote = false;

        // Toggle quote state and track if we saw any quotes
        if (c == '"') {
            in_quote = !in_quote;
            saw_quote = true;
            if (buf->len == 0)
                *initial_quote = true;
        }

        c = *(*lineptr)++;
    }

    // Back up one character (important for null terminator)
    (*lineptr)--;

    // Return true if we found a token (quoted or non-empty)
    return (saw_quote || buf->len > 0);
}
```

Key simplifications made:
- Added clear comments explaining each major section
- Grouped related logic together for better readability
- Preserved all quote handling and escape logic
- Maintained the complex state tracking needed for proper parsing
- Kept the efficient single-pass tokenization algorithm