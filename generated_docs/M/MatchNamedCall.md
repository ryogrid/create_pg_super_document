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
- : HeapTuple containing the pg_proc entry for the candidate function
- : Total number of arguments in the call (positional + named)
- : List of argument names for the named arguments in the call
- : Whether OUT arguments should be considered as part of the argument list
- : Number of arguments being considered (either proargtypes or proallargtypes length)
- : Output parameter - pointer to array mapping call positions to function parameter positions

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