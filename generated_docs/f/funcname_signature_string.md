# funcname_signature_string

## Location
[src/backend/parser/parse_func.c:1993-2029](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_func.c#L1993-L2029)

## Overview
Builds a human-readable string representing a function signature with its name and argument types, primarily used for error messages.

## Definition

```c
const char *
funcname_signature_string(const char *funcname, int nargs,
						  List *argnames, const Oid *argtypes)
```
## Detailed Description
The  function constructs a formatted string representation of a function signature that includes the function name followed by its argument types in parentheses. The result follows the format "foo(integer, text)" or "foo(x => integer, y => text)" when named arguments are involved. This function is particularly valuable for generating informative error messages when function lookup fails, as it provides users with a clear representation of what function signature was being sought.

The function handles both positional and named arguments, properly formatting named arguments with the "name => type" syntax. It distinguishes between positional arguments (which appear first) and named arguments based on the length of the  list and the total argument count.

## Parameters / Member Variables
- : The name of the function to include in the signature
- : Total number of arguments in the function signature
- : List of C strings containing the names for the last N arguments (can be NIL if no named args)
- : Array of OIDs representing the types of each argument

## Dependencies
- Functions called/Symbols referenced:
  - [initStringInfo](../i/initStringInfo.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [list_length](../l/list_length.md)
  - [list_head](../l/list_head.md)
  - [lnext](../l/lnext.md)
  - lfirst
  - [format_type_be](format_type_be.md)
- Called from (representative examples):
  - [func_signature_string](func_signature_string.md)
  - [IsThereFunctionInNamespace](../I/IsThereFunctionInNamespace.md)
  - [FuncDetailCode](../F/FuncDetailCode.md) (referenced from header)

## Notes and Other Information
- Returns a palloc'd string buffer that should be freed by the caller when no longer needed
- Handles mixed positional and named argument scenarios correctly
- Named arguments are always assumed to be the last N arguments in the signature
- Uses StringInfo for efficient string building and memory management
- The formatted output includes proper comma separation and spacing for readability
- Primarily used in error message construction when function resolution fails

## Simplified Source

```c
const char *
funcname_signature_string(const char *funcname, int nargs,
                          List *argnames, const Oid *argtypes)
{
    StringInfoData argbuf;
    int numposargs;
    ListCell *lc;
    int i;

    // Initialize string buffer for building function signature
    initStringInfo(&argbuf);
    appendStringInfo(&argbuf, "%s(", funcname);

    // Calculate how many arguments are positional vs named
    numposargs = nargs - list_length(argnames);
    lc = list_head(argnames);

    // Build argument list with types
    for (i = 0; i < nargs; i++)
    {
        if (i)
            appendStringInfoString(&argbuf, ", ");

        // For named arguments, add "name => " prefix
        if (i >= numposargs)
        {
            appendStringInfo(&argbuf, "%s => ", (char *) lfirst(lc));
            lc = lnext(argnames, lc);
        }

        // Add the type name
        appendStringInfoString(&argbuf, format_type_be(argtypes[i]));
    }

    appendStringInfoChar(&argbuf, ')');
    return argbuf.data;
}
```