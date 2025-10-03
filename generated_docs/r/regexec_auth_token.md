# regexec_auth_token

## Location
[src/backend/libpq/hba.c:346-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L346-L378)

## Overview
Executes a previously compiled regular expression from an AuthToken against a given string to check for pattern matches in PostgreSQL's authentication system.

## Definition

```c
static int
regexec_auth_token(const char *match, AuthToken *token, size_t nmatch,
				   regmatch_t pmatch[])
```
## Detailed Description
This function performs regular expression matching using a compiled regex stored in an AuthToken structure. It takes a target string and attempts to match it against the compiled regular expression pattern. The function handles multi-byte character encoding by converting the input match string to wide characters before executing the regex. It can optionally capture match groups by storing them in the provided pmatch array. This function is a critical component of PostgreSQL's host-based authentication system for pattern matching against database names, usernames, and other authentication parameters.

## Parameters / Member Variables
- `*match`: The input string to match against the regular expression
- `*token`: AuthToken containing the compiled regular expression to execute
- `nmatch`: Number of match groups to capture (size of pmatch array)
- `pmatch[]`: Array to store captured match groups (can be NULL if no capture needed)
## Dependencies
- Functions called/Symbols referenced:
  - : Converts multi-byte string to wide character array
  - : PostgreSQL's regex execution function
  - : Allocates memory for wide character string
  - : Frees allocated memory
- Called from (representative examples):
  - : For case-insensitive token matching
  - : When checking role names against patterns
  - : When checking database names against patterns
  - : When checking identity mapping rules

## Notes and Other Information
- Returns 0 on successful match, REG_NOMATCH if no match, or other error codes on failure
- Requires that the AuthToken's regex field is already compiled (asserts token->string[0] == '/' && token->regex)
- Handles multi-byte character sets properly by converting to wide characters
- Used throughout the authentication process for flexible pattern matching in HBA rules
- Part of PostgreSQL's authentication infrastructure in src/backend/libpq/hba.c
- Works in conjunction with regcomp_auth_token for complete regex functionality

## Simplified Source
```c
static int
regexec_auth_token(const char *match, AuthToken *token, size_t nmatch,
                   regmatch_t pmatch[])
{
    // Verify this is a compiled regex token
    Assert(token->string[0] == '/' && token->regex);

    // Convert match string to wide characters for multi-byte support
    pg_wchar *wmatchstr = palloc((strlen(match) + 1) * sizeof(pg_wchar));
    int wmatchlen = pg_mb2wchar_with_len(match, wmatchstr, strlen(match));

    // Execute the regex against the wide character string
    int result = pg_regexec(token->regex, wmatchstr, wmatchlen, 0, NULL,
                           nmatch, pmatch, 0);

    // Clean up allocated memory
    pfree(wmatchstr);
    return result;
}
```