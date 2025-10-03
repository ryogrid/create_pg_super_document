# recheck_cast_function_args

## Location
[src/backend/optimizer/util/clauses.c:4380-4424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L4380-L4424)

## Overview
Rechecks and typecasts function arguments as needed after default arguments have been added, ensuring polymorphic arguments are properly resolved and coerced.

## Definition

```c
static void
recheck_cast_function_args(List *args, Oid result_type,
						   Oid *proargtypes, int pronargs,
						   HeapTuple func_tuple)
```
## Detailed Description
This function handles the re-resolution of function arguments after default parameters have been added to a function call. When default arguments are added to a function call, some of these defaulted arguments may be polymorphic types that require re-resolution. The function ensures that all arguments have the correct data types by re-resolving polymorphic types and performing necessary type coercion, similar to what the parser originally did.

The function validates that the resolved return type matches the expected result type and performs any necessary typecasting of arguments. It modifies the args list in-place when casts are needed, so the caller should have already copied the list structure if preservation of the original is required.

## Parameters / Member Variables
- `*args`: List of function arguments to be rechecked and potentially recast
- `result_type`: Expected result type of the function call
- `*proargtypes`: Array of declared argument types for the function
- `pronargs`: Number of arguments the function expects
- `func_tuple`: HeapTuple containing the function's catalog entry (pg_proc)
## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc (struct type for pg_proc catalog entries)
  - FUNC_MAX_ARGS (maximum number of function arguments constant)
  - [enforce_generic_type_consistency](../e/enforce_generic_type_consistency.md) (resolves polymorphic types)
  - [make_fn_arguments](../m/make_fn_arguments.md) (performs argument type coercion)
- Called from:
  - [expand_function_arguments](../e/expand_function_arguments.md) (twice - for handling default arguments)

## Notes and Other Information
- This function is static and used internally within clauses.c
- The function performs validation to ensure the number of arguments doesn't exceed FUNC_MAX_ARGS
- It uses enforce_generic_type_consistency to handle polymorphic type resolution
- The function includes an assertion check to verify that the resolved return type matches the parser's original determination
- Located in src/backend/optimizer/util/clauses.c at lines 4380-4424

## Simplified Source

```c
static void
recheck_cast_function_args(List *args, Oid result_type,
                          Oid *proargtypes, int pronargs,
                          HeapTuple func_tuple)
{
    Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
    Oid actual_arg_types[FUNC_MAX_ARGS];
    Oid declared_arg_types[FUNC_MAX_ARGS];

    // Validate argument count
    if (list_length(args) > FUNC_MAX_ARGS)
        elog(ERROR, "too many function arguments");

    // Extract actual argument types from expression list
    int nargs = 0;
    ListCell *lc;
    foreach(lc, args)
    {
        actual_arg_types[nargs++] = exprType((Node *) lfirst(lc));
    }

    // Copy declared argument types
    memcpy(declared_arg_types, proargtypes, pronargs * sizeof(Oid));

    // Resolve polymorphic types and get final return type
    Oid rettype = enforce_generic_type_consistency(actual_arg_types,
                                                  declared_arg_types,
                                                  nargs,
                                                  funcform->prorettype,
                                                  false);

    // Verify return type consistency with parser's result
    if (rettype != result_type)
        elog(ERROR, "function's resolved result type changed during planning");

    // Apply necessary argument type casts
    make_fn_arguments(NULL, args, actual_arg_types, declared_arg_types);
}
```