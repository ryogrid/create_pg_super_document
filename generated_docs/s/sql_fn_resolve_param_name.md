# sql_fn_resolve_param_name

## Location
[src/backend/executor/functions.c:440-463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L440-L463)

## Overview
Searches for a function parameter by name and constructs a Param node if found, serving as a helper function for SQL function parameter resolution during parsing.

## Definition

```c
static Node *
sql_fn_resolve_param_name(SQLFunctionParseInfoPtr pinfo,
						  const char *paramname, int location)
```
## Detailed Description
This function implements parameter name resolution for SQL functions by searching through the argument names array in the parse info structure. When a parameter with the specified name is found, it delegates to sql_fn_make_param to construct the appropriate Param node. This is a key component of the SQL function parsing infrastructure that enables named parameter references in function bodies.

## Parameters / Member Variables
- `pinfo`: Pointer to SQLFunctionParseInfo structure containing function parsing context including argument names and count
- `*paramname`: Name of the parameter to search for
- `location`: Source location information for error reporting and node construction
## Dependencies
- Functions called/Symbols referenced:
  - [sql_fn_make_param](sql_fn_make_param.md)
  - SQLFunctionParseInfoPtr (type)
- Called from (representative examples):
  - [sql_fn_post_column_ref](sql_fn_post_column_ref.md)

## Notes and Other Information
- Returns NULL if no parameter with the given name is found or if argnames is NULL
- Uses 1-based parameter numbering when calling sql_fn_make_param (i + 1)
- Performs simple string comparison to match parameter names
- Part of the SQL function parsing hook infrastructure for resolving parameter references

## Simplified Source

```c
static Node *sql_fn_resolve_param_name(SQLFunctionParseInfoPtr pinfo,
                                       const char *paramname, int location) {
    int i;

    // Return NULL if no argument names are available
    if (pinfo->argnames == NULL)
        return NULL;

    // Search through all function arguments for matching name
    for (i = 0; i < pinfo->nargs; i++) {
        if (pinfo->argnames[i] && strcmp(pinfo->argnames[i], paramname) == 0) {
            // Found matching parameter, create Param node (convert to 1-based)
            return sql_fn_make_param(pinfo, i + 1, location);
        }
    }

    // No matching parameter found
    return NULL;
}
```