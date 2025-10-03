# MatchNamedCall

## Location
[src/backend/catalog/namespace.c:1585-1695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L1585-L1695)

## Overview
Determines whether a PostgreSQL function can match a call that uses named or mixed argument notation by validating argument names and positions.

## Definition

```c
static bool
MatchNamedCall(HeapTuple proctup, int nargs, List *argnames,
			   bool include_out_arguments, int pronargs,
			   int **argnumbers)
```
## Detailed Description
MatchNamedCall is a specialized function matching algorithm that handles calls using named argument notation (func(arg1 := value1, arg2 := value2)) or mixed notation (positional followed by named arguments). It validates that all supplied argument names correspond to actual function parameter names, that named arguments don't conflict with positional arguments, and that all unspecified arguments have defaults available.

The function creates a mapping array (argnumbers) that translates from call argument positions to actual function parameter positions, enabling proper argument reordering for named calls. It performs comprehensive validation including argument name resolution, position conflict detection, and default argument availability checking.

## Parameters / Member Variables
- `proctup`: HeapTuple containing the pg_proc entry for the candidate function
- `nargs`: Total number of arguments in the call (positional + named)
- `*argnames`: List of argument names for the named arguments in the call
- `include_out_arguments`: Whether OUT arguments should be considered as part of the argument list
- `pronargs`: Number of arguments being considered (either proargtypes or proallargtypes length)
- `**argnumbers`: Output parameter - pointer to array mapping call positions to function parameter positions
## Dependencies
- Functions called/Symbols referenced:
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [get_func_arg_info](../g/get_func_arg_info.md)
  - FUNC_PARAM_IN
  - FUNC_PARAM_INOUT
  - FUNC_PARAM_VARIADIC
- Called from (representative examples):
  - [FuncnameGetCandidates](../F/FuncnameGetCandidates.md)

## Notes and Other Information
- This is a static function only accessible within namespace.c
- Returns true on successful match, false if the function cannot match the named call
- The function handles both simple named calls and mixed positional/named calls
- Validates that named arguments come after all positional arguments
- Ensures no argument is specified both positionally and by name
- Critical for PostgreSQL's support of named and mixed argument notation in function calls
- The argnumbers array includes positions for defaulted arguments, enabling complete argument mapping
- Requires that the function has argument names defined (proargnames is not null)

## Simplified Source

```c
static bool MatchNamedCall(HeapTuple proctup, int nargs, List *argnames,
                          bool include_out_arguments, int pronargs, int **argnumbers) {
    Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);
    int numposargs = nargs - list_length(argnames);
    int pronallargs;
    Oid *p_argtypes;
    char **p_argnames;
    char *p_argmodes;
    bool arggiven[FUNC_MAX_ARGS];
    bool isnull;

    // Check if function has argument names
    (void) SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_proargnames, &isnull);
    if (isnull)
        return false;

    // Extract function argument information
    pronallargs = get_func_arg_info(proctup, &p_argtypes, &p_argnames, &p_argmodes);

    // Initialize argument tracking
    *argnumbers = (int *) palloc(pronargs * sizeof(int));
    memset(arggiven, false, pronargs * sizeof(bool));

    // Mark positional arguments as given
    for (int ap = 0; ap < numposargs; ap++) {
        (*argnumbers)[ap] = ap;
        arggiven[ap] = true;
    }

    // Process named arguments
    int ap = numposargs;
    foreach(lc, argnames) {
        char *argname = (char *) lfirst(lc);
        bool found = false;
        int pp = 0;

        // Find matching parameter name
        for (int i = 0; i < pronallargs; i++) {
            // Skip non-input parameters unless include_out_arguments is true
            if (!include_out_arguments && p_argmodes &&
                (p_argmodes[i] != FUNC_PARAM_IN &&
                 p_argmodes[i] != FUNC_PARAM_INOUT &&
                 p_argmodes[i] != FUNC_PARAM_VARIADIC))
                continue;

            if (p_argnames[i] && strcmp(p_argnames[i], argname) == 0) {
                // Check for conflict with positional argument
                if (arggiven[pp])
                    return false;

                arggiven[pp] = true;
                (*argnumbers)[ap] = pp;
                found = true;
                break;
            }
            pp++;
        }

        if (!found)
            return false;
        ap++;
    }

    // Fill in default arguments
    if (nargs < pronargs) {
        int first_arg_with_default = pronargs - procform->pronargdefaults;

        for (int pp = numposargs; pp < pronargs; pp++) {
            if (!arggiven[pp]) {
                if (pp < first_arg_with_default)
                    return false;
                (*argnumbers)[ap++] = pp;
            }
        }
    }

    return true;
}
```