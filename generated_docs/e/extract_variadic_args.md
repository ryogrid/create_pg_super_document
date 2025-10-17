# extract_variadic_args

## Location
[src/backend/utils/fmgr/funcapi.c:2005-2101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L2005-L2101)

## Overview
Extracts argument values, types, and NULL markers for functions that use VARIADIC parameters, handling both variadic array arguments and regular argument lists with proper type conversion and validation.

## Definition

```c
struct_array(array_in, element_type, typlen, typbyval,
						  typalign, &args_res, &nulls_res,
						  &nargs);
```
## Detailed Description
This function processes arguments for PostgreSQL functions that accept VARIADIC parameters. It handles two distinct scenarios:

1. **VARIADIC call**: When the function is called with a VARIADIC argument, the caller provides a single array containing all the variadic values. The function deconstructs this array to extract individual elements.

2. **Regular call**: When called without VARIADIC syntax, it processes the individual arguments starting from the variadic position.

The function also handles type conversion for UNKNOWN types when requested, converting them to TEXT when they represent constant literal values. This is particularly important for functions declared to accept "any" type, where the parser doesn't perform automatic type conversion on undecorated string literals.

The function allocates memory for the output arrays and populates them with the extracted argument data, ensuring proper type information is maintained throughout the process.

## Parameters / Member Variables
- : Function call information structure containing argument data and metadata
- : Starting position index for variadic arguments in the argument list  
- : Flag indicating whether to convert UNKNOWN type arguments to TEXT
- : Output pointer to array of Datum values for the extracted arguments
- : Output pointer to array of Oid type identifiers for each argument
- : Output pointer to array of boolean NULL indicators for each argument

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_variadic](../g/get_fn_expr_variadic.md)
  - PG_NARGS
  - PG_GETARG_ARRAYTYPE_P
  - ARR_ELEMTYPE
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - [deconstruct_array](../d/deconstruct_array.md)
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md)
  - [get_fn_expr_arg_stable](../g/get_fn_expr_arg_stable.md)
  - CStringGetTextDatum
  - PG_GETARG_DATUM
  - PG_ARGISNULL
  - PG_GETARG_POINTER
- Called from (representative examples):
  - [json_build_object](../j/json_build_object.md)
  - [json_build_array](../j/json_build_array.md)
  - [jsonb_build_object](../j/jsonb_build_object.md)
  - [jsonb_build_array](../j/jsonb_build_array.md)

## Notes and Other Information
- Returns the number of arguments extracted, or -1 for "VARIADIC NULL" case
- Memory allocation is performed using palloc0() for all output arrays
- The function performs strict validation and will report errors for invalid argument types
- When processing VARIADIC arrays, all elements are assumed to have the same type as the array element type
- Type conversion from UNKNOWN to TEXT only occurs for stable constant expressions
- Used extensively in JSON construction functions that accept variable numbers of key-value pairs or array elements

## Simplified Source

```c
int extract_variadic_args(FunctionCallInfo fcinfo, int variadic_start,
                         bool convert_unknown, Datum **args, Oid **types, bool **nulls) {
    bool variadic = get_fn_expr_variadic(fcinfo->flinfo);
    Datum *args_res;
    bool *nulls_res;
    Oid *types_res;
    int nargs;

    // Initialize output parameters
    *args = NULL;
    *types = NULL;
    *nulls = NULL;

    if (variadic) {
        // Handle VARIADIC call - single array argument
        Assert(PG_NARGS() == variadic_start + 1);

        if (PG_ARGISNULL(variadic_start))
            return -1;  // VARIADIC NULL case

        ArrayType *array_in = PG_GETARG_ARRAYTYPE_P(variadic_start);
        Oid element_type = ARR_ELEMTYPE(array_in);

        // Get type properties and deconstruct array
        bool typbyval;
        char typalign;
        int16 typlen;
        get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
        deconstruct_array(array_in, element_type, typlen, typbyval, typalign,
                         &args_res, &nulls_res, &nargs);

        // All elements have the same type
        types_res = (Oid *) palloc0(nargs * sizeof(Oid));
        for (int i = 0; i < nargs; i++)
            types_res[i] = element_type;
    }
    else {
        // Handle regular call - process individual arguments
        nargs = PG_NARGS() - variadic_start;
        Assert(nargs > 0);

        nulls_res = (bool *) palloc0(nargs * sizeof(bool));
        args_res = (Datum *) palloc0(nargs * sizeof(Datum));
        types_res = (Oid *) palloc0(nargs * sizeof(Oid));

        for (int i = 0; i < nargs; i++) {
            nulls_res[i] = PG_ARGISNULL(i + variadic_start);
            types_res[i] = get_fn_expr_argtype(fcinfo->flinfo, i + variadic_start);

            // Convert UNKNOWN types to TEXT when requested
            if (convert_unknown && types_res[i] == UNKNOWNOID &&
                get_fn_expr_arg_stable(fcinfo->flinfo, i + variadic_start)) {
                types_res[i] = TEXTOID;

                if (PG_ARGISNULL(i + variadic_start))
                    args_res[i] = (Datum) 0;
                else
                    args_res[i] = CStringGetTextDatum(PG_GETARG_POINTER(i + variadic_start));
            }
            else {
                // Use argument as-is
                args_res[i] = PG_GETARG_DATUM(i + variadic_start);
            }

            // Validate type resolution
            if (!OidIsValid(types_res[i]) || (convert_unknown && types_res[i] == UNKNOWNOID))
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("could not determine data type for argument %d", i + 1)));
        }
    }

    // Return results
    *args = args_res;
    *nulls = nulls_res;
    *types = types_res;

    return nargs;
}
```