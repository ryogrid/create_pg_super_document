# pltcl_quote

## Location
[src/pl/tcl/pltcl.c:2009-2062](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L2009-L2062)

## Overview
Escapes literal strings for safe inclusion in SPI_execute query strings by doubling single quotes and backslashes.

## Definition
```c
static int
pltcl_quote(ClientData cdata, Tcl_Interp *interp,
            int objc, Tcl_Obj *const objv[])
```

## Detailed Description
This function implements a Tcl command that properly escapes string literals to prevent SQL injection when constructing dynamic SQL queries in PL/Tcl functions. It processes a string by doubling any single quotes (') and backslashes (\) found within it, which is the standard PostgreSQL method for escaping these characters in string literals.

The function follows the Tcl command interface pattern, accepting arguments through the objv array and returning results through the Tcl interpreter. It validates that exactly one string argument is provided, processes the string character by character, and returns the escaped version as the Tcl command result.

The escaping logic allocates a buffer of up to twice the original string length (worst case where every character needs escaping) and walks through the input string, copying each character while adding an extra quote or backslash when these special characters are encountered.

## Parameters / Member Variables
- `cdata`: ClientData passed to the Tcl command (unused)
- `interp`: Tcl interpreter context for setting results and errors
- `objc`: Number of arguments passed to the command (should be 2: command name + string)
- `objv`: Array of Tcl objects containing the command arguments

## Dependencies
- Functions called/Symbols referenced:
  - Tcl_WrongNumArgs (Tcl library function)
  - Tcl_GetStringFromObj (Tcl library function)
  - [palloc](palloc.md) (PostgreSQL memory allocator)
  - Tcl_SetObjResult (Tcl library function)
  - Tcl_NewStringObj (Tcl library function)
  - [pfree](pfree.md) (PostgreSQL memory deallocator)
  - Tcl_Size (Tcl type definition)
- Called from (representative examples):
  - TclExceptionNameMap (registered as Tcl command)
  - [pltcl_init_interp](pltcl_init_interp.md) (command registration)

## Notes and Other Information
- Returns TCL_ERROR if wrong number of arguments provided
- Returns TCL_OK on successful escaping
- Doubles single quotes (') and backslashes (\) for SQL safety
- Uses PostgreSQL's palloc/pfree for memory management
- Registered as a built-in Tcl command "quote" available to PL/Tcl functions
- Essential for building dynamic SQL queries safely from within Tcl stored procedures
- The escaped string is returned as the Tcl command result