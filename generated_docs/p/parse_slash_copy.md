# parse_slash_copy

## Location
[src/bin/psql/copy.c:89-267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/copy.c#L89-L267)

## Overview
Parses the arguments of a psql \copy command line and returns a structured representation of the parsed options.

## Definition
```c
static struct copy_options *
parse_slash_copy(const char *args)
```

## Detailed Description
This function parses the complex syntax of psql's \copy command, which supports various forms including table names with optional schema and column lists, query expressions in parentheses, and different file/stream destinations. It handles backward compatibility with the deprecated BINARY keyword, processes schema-qualified table names, column lists in parentheses, FROM/TO direction specification, and various file/program/stream options (STDIN/STDOUT/PSTDIN/PSTDOUT/PROGRAM). The parser uses strtokx for tokenization with proper quote and delimiter handling.

## Parameters / Member Variables
- `args`: String containing the command line arguments for the \copy command

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc0](pg_malloc0.md), pg_strdup (PostgreSQL memory allocation)
  - [strtokx](../s/strtokx.md) (PostgreSQL tokenizer)
  - [pg_strcasecmp](pg_strcasecmp.md) (PostgreSQL string comparison)
  - [xstrcat](../x/xstrcat.md) (local utility function)
  - [strip_quotes](../s/strip_quotes.md), expand_tilde (path processing)
  - pg_log_error (error reporting)
  - [free_copy_options](../f/free_copy_options.md) (cleanup function)
  - [standard_strings](../s/standard_strings.md)() (PostgreSQL configuration check)
- Called from (representative examples):
  - [do_copy](../d/do_copy.md) (src/bin/psql/copy.c)

## Notes and Other Information
- Supports both legacy 7.3 syntax (with BINARY keyword) and modern syntax
- Handles complex SQL query expressions enclosed in parentheses
- Processes schema-qualified table names (schema.table format)
- Supports column lists in parentheses after table names
- Recognizes special file destinations: STDIN, STDOUT, PSTDIN, PSTDOUT, and PROGRAM commands
- Returns NULL on parsing errors with appropriate error messages logged
- The function builds SQL command components in before_tofrom and after_tofrom fields
- Critical component of psql's \copy command infrastructure

## Simplified Source

```c
static struct copy_options *parse_slash_copy(const char *args)
{
    struct copy_options *result;
    char *token;
    const char *whitespace = " \t\n\r";
    char nonstd_backslash = standard_strings() ? 0 : '\\';

    if (!args) {
        pg_log_error("\\copy: arguments required");
        return NULL;
    }

    result = pg_malloc0(sizeof(struct copy_options));
    result->before_tofrom = pg_strdup(""); // Initialize for appending

    // Get first token
    token = strtokx(args, whitespace, ".,()", "\"",
                   0, false, false, pset.encoding);
    if (!token)
        goto error;

    // Handle legacy BINARY keyword (7.3 compatibility)
    if (pg_strcasecmp(token, "binary") == 0) {
        xstrcat(&result->before_tofrom, token);
        token = strtokx(NULL, whitespace, ".,()", "\"",
                       0, false, false, pset.encoding);
        if (!token)
            goto error;
    }

    // Handle COPY (query) case
    if (token[0] == '(') {
        int parens = 1;

        while (parens > 0) {
            xstrcat(&result->before_tofrom, " ");
            xstrcat(&result->before_tofrom, token);
            token = strtokx(NULL, whitespace, "()", "\"'",
                           nonstd_backslash, true, false, pset.encoding);
            if (!token)
                goto error;
            if (token[0] == '(')
                parens++;
            else if (token[0] == ')')
                parens--;
        }
    }

    // Add current token and get next
    xstrcat(&result->before_tofrom, " ");
    xstrcat(&result->before_tofrom, token);
    token = strtokx(NULL, whitespace, ".,()", "\"",
                   0, false, false, pset.encoding);
    if (!token)
        goto error;

    // Handle schema.table syntax
    if (token[0] == '.') {
        xstrcat(&result->before_tofrom, token);
        token = strtokx(NULL, whitespace, ".,()", "\"",
                       0, false, false, pset.encoding);
        if (!token)
            goto error;
        xstrcat(&result->before_tofrom, token);
        token = strtokx(NULL, whitespace, ".,()", "\"",
                       0, false, false, pset.encoding);
        if (!token)
            goto error;
    }

    // Handle column list in parentheses
    if (token[0] == '(') {
        for (;;) {
            xstrcat(&result->before_tofrom, " ");
            xstrcat(&result->before_tofrom, token);
            token = strtokx(NULL, whitespace, "()", "\"",
                           0, false, false, pset.encoding);
            if (!token)
                goto error;
            if (token[0] == ')')
                break;
        }
        xstrcat(&result->before_tofrom, " ");
        xstrcat(&result->before_tofrom, token);
        token = strtokx(NULL, whitespace, ".,()", "\"",
                       0, false, false, pset.encoding);
        if (!token)
            goto error;
    }

    // Parse FROM/TO direction
    if (pg_strcasecmp(token, "from") == 0)
        result->from = true;
    else if (pg_strcasecmp(token, "to") == 0)
        result->from = false;
    else
        goto error;

    // Parse file/stream destination
    token = strtokx(NULL, whitespace, ";", "'",
                   0, false, false, pset.encoding);
    if (!token)
        goto error;

    if (pg_strcasecmp(token, "program") == 0) {
        // Handle PROGRAM 'command'
        token = strtokx(NULL, whitespace, ";", "'",
                       0, false, false, pset.encoding);
        if (!token)
            goto error;

        int toklen = strlen(token);
        if (token[0] != '\'' || toklen < 2 || token[toklen - 1] != '\'')
            goto error;

        strip_quotes(token, '\'', 0, pset.encoding);
        result->program = true;
        result->file = pg_strdup(token);
    }
    else if (pg_strcasecmp(token, "stdin") == 0 ||
             pg_strcasecmp(token, "stdout") == 0) {
        result->file = NULL;
    }
    else if (pg_strcasecmp(token, "pstdin") == 0 ||
             pg_strcasecmp(token, "pstdout") == 0) {
        result->psql_inout = true;
        result->file = NULL;
    }
    else {
        // Regular filename (optionally quoted)
        strip_quotes(token, '\'', 0, pset.encoding);
        result->file = pg_strdup(token);
        expand_tilde(&result->file);
    }

    // Collect remaining options
    token = strtokx(NULL, "", NULL, NULL,
                   0, false, false, pset.encoding);
    if (token)
        result->after_tofrom = pg_strdup(token);

    return result;

error:
    if (token)
        pg_log_error("\\copy: parse error at \"%s\"", token);
    else
        pg_log_error("\\copy: parse error at end of line");
    free_copy_options(result);
    return NULL;
}
```