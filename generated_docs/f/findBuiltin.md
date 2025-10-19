# findBuiltin

## Location
[src/bin/pgbench/pgbench.c:6156-6191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L6156-L6191)

## Overview
Searches for a built-in benchmark script by name, supporting partial name matching, and returns the script if unambiguous.

## Definition
```c
static const BuiltinScript *findBuiltin(const char *name)
```

## Detailed Description
The `findBuiltin` function implements a smart lookup mechanism for built-in benchmark scripts in pgbench. It accepts a script name (or partial name prefix) and searches through the global `builtin_script` array using prefix matching. If exactly one script matches the provided name/prefix, it returns a pointer to that BuiltinScript. If no matches are found or multiple scripts match (ambiguous), it logs an appropriate error message, displays the list of available scripts, and terminates the program. This allows users to specify built-in scripts using abbreviated names as long as they are unambiguous.

## Parameters / Member Variables
- `name`: The name or prefix of the built-in script to search for

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard library)
  - strncmp (standard library) 
  - lengthof (macro)
  - pg_log_error
  - [listAvailableScripts](../l/listAvailableScripts.md)
  - exit (standard library)
  - builtin_script (global array)
- Called from (representative examples):
  - [main](../m/main.md) (multiple locations in src/bin/pgbench/pgbench.c: 6786, 6887, 6928, 6994, 7052)

## Notes and Other Information
- Uses prefix matching, allowing users to specify partial script names (e.g., "tpc" for "tpcb-like")
- Returns NULL only in error cases before calling exit(1), making it effectively non-NULL for successful calls
- Error handling includes specific messages for "not found" vs "ambiguous" cases
- Automatically displays available scripts list when lookup fails to help users
- Located in src/bin/pgbench/pgbench.c at lines 6156-6191
- Multiple calls from main() indicate this is used extensively for script selection in various benchmark scenarios
- The function ensures user-friendly script selection while maintaining precision through disambiguation

## Simplified Source

```c
static const BuiltinScript *
findBuiltin(const char *name)
{
    int found = 0;
    int len = strlen(name);
    const BuiltinScript *result = NULL;

    // Search through all built-in scripts using prefix matching
    for (int i = 0; i < lengthof(builtin_script); i++)
    {
        if (strncmp(builtin_script[i].name, name, len) == 0)
        {
            result = &builtin_script[i];
            found++;
        }
    }

    // Return if exactly one match found (unambiguous)
    if (found == 1)
        return result;

    // Handle error cases: no match or multiple matches
    if (found == 0)
        pg_log_error("no builtin script found for name \"%s\"", name);
    else
        pg_log_error("ambiguous builtin name: %d builtin scripts found for prefix \"%s\"", found, name);

    // Show available options and exit
    listAvailableScripts();
    exit(1);
}
```