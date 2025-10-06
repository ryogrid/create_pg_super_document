# pg_get_function_arg_default

## Location
[src/backend/utils/adt/ruleutils.c:3440-3509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L3440-L3509)

## Overview
A PostgreSQL SQL function that returns the textual representation of a function argument's default value for a specific argument position.

## Definition
```c
Datum pg_get_function_arg_default(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves and formats the default value of a function argument based on the function OID and argument position. It takes a function ID and an argument number (1-based indexing among all arguments, including OUT parameters) and returns the SQL representation of that argument's default value. The function handles the complex logic of mapping argument positions to default value positions, since default values only apply to input arguments and are stored in the order of the last N input arguments where N is the number of arguments with defaults.

## Parameters / Member Variables
- `funcid` (PG_GETARG_OID(0)): The OID of the function to examine
- `nth_arg` (PG_GETARG_INT32(1)): The 1-based argument position among all arguments (proallargtypes)

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc
  - [get_func_arg_info](../g/get_func_arg_info.md)
  - [is_input_argument](../i/is_input_argument.md) (called twice)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - TextDatumGetCString
  - [stringToNode](../s/stringToNode.md)
  - [list_nth](../l/list_nth.md)
  - [deparse_expression](../d/deparse_expression.md)
  - [string_to_text](../s/string_to_text.md)
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct references found (likely called via SQL interface)

## Notes and Other Information
- Uses 1-based indexing for argument positions to match information_schema.sql conventions
- Only works with input arguments (IN, INOUT, VARIADIC modes)
- Returns NULL if the function doesn't exist, argument position is invalid, or no default value exists
- The function performs complex index calculations since proargdefaults only stores the last N input arguments that have defaults
- Default values are stored as serialized Node structures and are deserialized and deparsed back to SQL text
- This function is typically exposed to SQL as pg_get_function_arg_default(funcid, argnum)

## Simplified Source

```c
Datum pg_get_function_arg_default(PG_FUNCTION_ARGS) {
    Oid funcid = PG_GETARG_OID(0);
    int32 nth_arg = PG_GETARG_INT32(1);
    HeapTuple proctup;
    Form_pg_proc proc;
    int numargs;
    Oid *argtypes;
    char **argnames;
    char *argmodes;
    int i;
    List *argdefaults;
    Node *node;
    char *str;
    int nth_inputarg;
    Datum proargdefaults;
    bool isnull;
    int nth_default;

    // Look up the function in pg_proc
    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup))
        PG_RETURN_NULL();

    // Get function argument information
    numargs = get_func_arg_info(proctup, &argtypes, &argnames, &argmodes);

    // Validate argument position and ensure it's an input argument
    if (nth_arg < 1 || nth_arg > numargs || !is_input_argument(nth_arg - 1, argmodes)) {
        ReleaseSysCache(proctup);
        PG_RETURN_NULL();
    }

    // Count input arguments up to nth_arg to find the input position
    nth_inputarg = 0;
    for (i = 0; i < nth_arg; i++)
        if (is_input_argument(i, argmodes))
            nth_inputarg++;

    // Get the function's argument defaults
    proargdefaults = SysCacheGetAttr(PROCOID, proctup,
                                    Anum_pg_proc_proargdefaults, &isnull);
    if (isnull) {
        ReleaseSysCache(proctup);
        PG_RETURN_NULL();
    }

    // Parse the defaults list
    str = TextDatumGetCString(proargdefaults);
    argdefaults = castNode(List, stringToNode(str));
    pfree(str);

    proc = (Form_pg_proc) GETSTRUCT(proctup);

    // Calculate index into proargdefaults (corresponds to last N input args)
    nth_default = nth_inputarg - 1 - (proc->pronargs - proc->pronargdefaults);

    // Validate the default index and extract the default expression
    if (nth_default < 0 || nth_default >= list_length(argdefaults)) {
        ReleaseSysCache(proctup);
        PG_RETURN_NULL();
    }

    node = list_nth(argdefaults, nth_default);
    str = deparse_expression(node, NIL, false, false);

    ReleaseSysCache(proctup);
    PG_RETURN_TEXT_P(string_to_text(str));
}
```