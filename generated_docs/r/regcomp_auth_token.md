# regcomp_auth_token

## Location
[src/backend/libpq/hba.c:301-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L301-L345)

## Overview
Compiles a regular expression string stored in an AuthToken and stores the compiled regex in the token's regex field for later use in authentication pattern matching.

## Definition

```c
static int
regcomp_auth_token(AuthToken *token, char *filename, int line_num,
				   char **err_msg, int elevel)
```
## Detailed Description
This function handles the compilation of regular expression patterns used in PostgreSQL's host-based authentication (HBA) system. It takes a string from an AuthToken that begins with '/' (indicating a regex pattern) and compiles it using PostgreSQL's internal regex engine. The function converts the input string from the database encoding to wide characters before compilation, which allows proper handling of multi-byte character sets. If compilation fails, it generates detailed error messages including the line number and filename from the configuration file where the pattern was found.

## Parameters / Member Variables
- `*token`: AuthToken structure containing the regex string to compile and where the compiled regex will be stored
- `*filename`: Name of the configuration file being processed (used for error reporting)
- `line_num`: Line number in the configuration file (used for error reporting)
- `**err_msg`: Pointer to store error message string on compilation failure
- `elevel`: Error level for logging (e.g., ERROR, LOG, WARNING)
## Dependencies
- Functions called/Symbols referenced:
  - : Converts multi-byte string to wide character array
  - : PostgreSQL's regex compilation function
  - : Gets error message from failed regex compilation
  - : Allocates zero-initialized memory
  - : Frees allocated memory
  - : Reports errors with specified level
  - : Provides context information for error reporting
- Called from (representative examples):
  - : When parsing HBA configuration lines
  - : When parsing identity mapping configuration lines
  - : During token matching operations

## Notes and Other Information
- Returns 0 on success, non-zero error code on failure
- Only processes tokens whose string starts with '/' character
- Uses REG_ADVANCED regex compilation flags for extended regex features
- Allocates memory for the regex_t structure that must be freed later
- Provides comprehensive error reporting with file context for debugging configuration issues
- Part of PostgreSQL's authentication infrastructure in src/backend/libpq/hba.c

## Simplified Source

```c
// Simplified version of regcomp_auth_token
static int regcomp_auth_token(AuthToken *token, char *filename, int line_num,
                             char **err_msg, int elevel) {
    // Check if this token contains a regex pattern (starts with '/')
    if (token->string[0] != '/') {
        return 0;  // Not a regex pattern, nothing to compile
    }

    // Allocate memory for the compiled regex
    token->regex = (regex_t *) palloc0(sizeof(regex_t));

    // Convert the regex string to wide characters for proper multi-byte handling
    pg_wchar *wide_string = palloc((strlen(token->string + 1) + 1) * sizeof(pg_wchar));
    int wide_length = pg_mb2wchar_with_len(token->string + 1, wide_string,
                                          strlen(token->string + 1));

    // Compile the regular expression using PostgreSQL's regex engine
    int result = pg_regcomp(token->regex, wide_string, wide_length,
                           REG_ADVANCED, C_COLLATION_OID);

    // Handle compilation errors
    if (result != 0) {
        char error_buffer[100];
        pg_regerror(result, token->regex, error_buffer, sizeof(error_buffer));

        // Report error with file context
        ereport(elevel,
                (errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
                 errmsg("invalid regular expression \"%s\": %s",
                        token->string + 1, error_buffer),
                 errcontext("line %d of configuration file \"%s\"",
                           line_num, filename)));

        // Store error message for caller
        *err_msg = psprintf("invalid regular expression \"%s\": %s",
                           token->string + 1, error_buffer);
    }

    // Clean up temporary wide character string
    pfree(wide_string);
    return result;
}
```

Key simplifications made:
- Added descriptive comments for each logical step
- Used more readable variable names (wide_string, wide_length, error_buffer, result)
- Grouped related operations together logically
- Maintained all essential functionality including error handling
- Preserved the core algorithm: check pattern type → allocate memory → convert encoding → compile regex → handle errors → cleanup