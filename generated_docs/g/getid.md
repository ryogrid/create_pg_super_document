# getid

## Location
[src/backend/utils/adt/acl.c:165-217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L165-L217)

## Overview
Parses the first identifier from a string, handling both quoted and unquoted identifiers while ignoring leading whitespace, and returns the position after the parsed identifier.

## Definition

```c
static const char *
getid(const char *s, char *n, Node *escontext)
```
## Detailed Description
This function extracts identifiers from ACL strings during parsing operations. It handles two types of identifiers: unquoted alphanumeric identifiers (including underscores and high-bit characters) and quoted identifiers enclosed in double quotes. For quoted identifiers, it processes escaped quotes (double quotes within quotes) correctly.

The function implements robust error handling through the escontext mechanism, allowing callers to choose between immediate error reporting (ereport) or error logging for later processing. It ensures identifier length compliance with PostgreSQL's NAMEDATALEN limit and handles whitespace trimming both before and after identifier parsing.

## Parameters / Member Variables
- `*s`: Input string to parse, positioned at or before the identifier
- `*n`: Output buffer to store the parsed identifier (must be NAMEDATALEN bytes)
- `*escontext`: Error context node for error handling - if ErrorSaveData, errors are logged rather than thrown
## Dependencies
- Functions called/Symbols referenced:
  - [is_safe_acl_char](../i/is_safe_acl_char.md) (determines if character is safe in ACL identifiers)
  - NAMEDATALEN (maximum identifier length constant)
  - ereturn (error handling macro)
  - isspace (standard C library function)
- Called from (representative examples):
  - [aclparse](../a/aclparse.md) (multiple call sites for parsing ACL components)

## Notes and Other Information
The function carefully handles quoted identifiers with escape sequences, where two consecutive double quotes within a quoted identifier represent a literal double quote character. The parsing state machine tracks whether currently inside quotes to properly handle quote characters. Length validation prevents buffer overflows and ensures compliance with PostgreSQL's identifier length restrictions.

## Simplified Source

```c
static const char *getid(const char *s, char *n, Node *escontext) {
    int len = 0;
    bool in_quotes = false;

    // Skip leading whitespace
    while (isspace(*s))
        s++;

    // Parse identifier characters
    while (*s != '\0' && (in_quotes || *s == '"' || is_safe_acl_char(*s, true))) {
        if (*s == '"') {
            if (!in_quotes) {
                in_quotes = true;  // Start quoted section
                s++;
                continue;
            }
            // Check for escaped quote (double quote)
            if (*(s + 1) != '"') {
                in_quotes = false;  // End quoted section
                s++;
                continue;
            }
            s++;  // Skip escape character
        }

        // Check identifier length limit
        if (len >= NAMEDATALEN - 1) {
            return ereturn(escontext, NULL,
                          (errcode(ERRCODE_NAME_TOO_LONG),
                           errmsg("identifier too long")));
        }

        n[len++] = *s++;
    }

    n[len] = '\0';

    // Skip trailing whitespace
    while (isspace(*s))
        s++;

    return s;
}
```