# reorder_function_arguments

## Location
[src/backend/optimizer/util/clauses.c:4256-4325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L4256-L4325)

## Overview
Converts named-notation function arguments to positional notation and inserts default argument values to create a properly ordered argument list.

## Definition

```c
struct the argument list into an array indexed by argnumber */
	i = 0;
```
## Detailed Description
This function handles the conversion of function arguments from named notation to positional notation. It processes argument lists that may contain a mix of positional and named arguments, where positional arguments must precede all named arguments.

The function operates in several phases:
1. **Validation**: Ensures the number of arguments doesn't exceed system limits (FUNC_MAX_ARGS)
2. **Deconstruction**: Maps both positional and named arguments into an array indexed by parameter position
3. **Default insertion**: Fills in missing arguments with their default values using fetch_function_defaults
4. **Reconstruction**: Builds a new argument list in proper positional order

The function assumes that positional arguments appear before named arguments in the input list and validates that named arguments specify valid parameter positions.

## Parameters / Member Variables
- : Input list containing mix of positional and named arguments
- : Total number of parameters the function expects
- : The function's pg_proc tuple for accessing default values

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc
  - FUNC_MAX_ARGS
  - [NamedArgExpr](../N/NamedArgExpr.md)
  - [fetch_function_defaults](../f/fetch_function_defaults.md)
- Called from (representative examples):
  - [expand_function_arguments](../e/expand_function_arguments.md)

## Notes and Other Information
- The function enforces PostgreSQL's argument limit of FUNC_MAX_ARGS (100) parameters
- Named arguments must specify valid argnumber values within the function's parameter range
- Default values are fetched only when needed (when fewer arguments are provided than required)
- The function creates a new argument list rather than modifying the input list
- All argument positions must be filled after processing; NULL entries in the final array indicate an error condition
- Default argument expressions are inserted at positions corresponding to parameters that have defaults defined

## Simplified Source

```c
static List *
reorder_function_arguments(List *args, int pronargs, HeapTuple func_tuple)
{
    Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
    int nargsprovided = list_length(args);
    Node *argarray[FUNC_MAX_ARGS];

    // Validate argument count and initialize array
    if (pronargs < 0 || pronargs > FUNC_MAX_ARGS)
        elog(ERROR, "too many function arguments");
    memset(argarray, 0, pronargs * sizeof(Node *));

    // Place arguments into array by position
    int i = 0;
    ListCell *lc;
    foreach(lc, args)
    {
        Node *arg = (Node *) lfirst(lc);

        if (!IsA(arg, NamedArgExpr))
        {
            // Positional argument - place sequentially
            argarray[i++] = arg;
        }
        else
        {
            // Named argument - place at specified position
            NamedArgExpr *na = (NamedArgExpr *) arg;
            argarray[na->argnumber] = (Node *) na->arg;
        }
    }

    // Fill in missing arguments with defaults
    if (nargsprovided < pronargs)
    {
        List *defaults = fetch_function_defaults(func_tuple);

        // Insert defaults starting from first parameter with default
        i = pronargs - funcform->pronargdefaults;
        foreach(lc, defaults)
        {
            if (argarray[i] == NULL)
                argarray[i] = (Node *) lfirst(lc);
            i++;
        }
    }

    // Rebuild argument list in positional order
    args = NIL;
    for (i = 0; i < pronargs; i++)
    {
        args = lappend(args, argarray[i]);
    }

    return args;
}
```