# convertRegProcReference

## Location
[src/bin/pg_dump/pg_dump.c:13181-13221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L13181-L13221)

## Overview
Converts a function reference obtained from pg_operator by stripping the argument-types part from a REGPROCEDURE display string.

## Definition

```c
static char *
convertRegProcReference(const char *proc)
```
## Detailed Description
This function processes REGPROCEDURE display strings that include both function names and their argument types. It extracts just the function name portion by locating the first non-quoted left parenthesis and truncating the string at that point. The function handles quoted identifiers properly by tracking quote state to avoid splitting on parentheses that appear within quoted function names.

The function returns a dynamically allocated string containing only the function name, or NULL if the input represents an invalid OID (indicated by "-"). The caller is responsible for freeing the returned string.

## Parameters / Member Variables
- `*proc`: Input REGPROCEDURE display string containing function name and argument types
## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) (for string duplication)
  - strcmp (for string comparison)
- Called from (representative examples):
  - [dumpOpr](../d/dumpOpr.md) (multiple calls for different operator functions)
  - fmtQualifiedDumpable

## Notes and Other Information
- Returns NULL for invalid OID references (represented as "-")
- Properly handles quoted function names by tracking quote state
- The returned string must be freed by the caller
- Used specifically in the context of pg_dump for formatting operator function references
- Part of PostgreSQL's database dumping functionality in pg_dump utility

## Simplified Source

```c
static char *
convertRegProcReference(const char *proc)
{
    char *name;
    char *paren;
    bool inquote;

    // Handle invalid OID references
    if (strcmp(proc, "-") == 0)
        return NULL;

    name = pg_strdup(proc);

    // Find non-quoted left parenthesis to strip argument types
    inquote = false;
    for (paren = name; *paren; paren++) {
        if (*paren == '(' && !inquote) {
            *paren = '\0';  // Truncate at first non-quoted opening paren
            break;
        }
        if (*paren == '"')
            inquote = !inquote;  // Track quote state
    }

    return name;
}
```