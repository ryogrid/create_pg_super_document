# concat_internal

## Location
[src/backend/utils/adt/varlena.c:5422-5501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L5422-L5501)

## Overview
Core implementation function for both concat() and concat_ws() operations, handling the concatenation of multiple arguments with an optional separator string.

## Definition
```c
static text *concat_internal(const char *sepstr, int argidx, FunctionCallInfo fcinfo)
```

## Detailed Description
This static function serves as the unified implementation for PostgreSQL's concatenation functions. It handles two main scenarios: VARIADIC array concatenation (delegating to array_to_text_internal) and normal multi-argument concatenation. The function builds a StringInfo buffer, processes each non-NULL argument starting from the specified index, converts each argument to its string representation using cached output functions, and combines them with the provided separator. NULL arguments are ignored during concatenation.

## Parameters / Member Variables
- `sepstr`: Separator string to place between concatenated values (can be empty string for concat())
- `argidx`: Starting argument index for concatenation (must be constant across call series)
- `fcinfo`: Function call information containing arguments and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_variadic](../g/get_fn_expr_variadic.md)
  - PG_NARGS
  - [get_base_element_type](../g/get_base_element_type.md)
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md)
  - PG_GETARG_ARRAYTYPE_P
  - [array_to_text_internal](../a/array_to_text_internal.md)
  - [build_concat_foutcache](../b/build_concat_foutcache.md)
  - [OutputFunctionCall](../O/OutputFunctionCall.md)
  - [cstring_to_text_with_len](cstring_to_text_with_len.md)
- Called from (representative examples):
  - [text_concat](../t/text_concat.md)
  - [text_concat_ws](../t/text_concat_ws.md)

## Notes and Other Information
- Returns NULL if the result should be NULL, otherwise returns a text value
- Handles VARIADIC array arguments by delegating to array_to_text_internal
- Uses cached output function information for performance optimization
- Ignores NULL arguments during concatenation process
- Memory management includes proper cleanup of StringInfo buffer
- The argidx parameter must remain constant across multiple calls for proper caching behavior

## Simplified Source

```c
static text *
concat_internal(const char *sepstr, int argidx, FunctionCallInfo fcinfo)
{
    text *result;
    StringInfoData str;
    FmgrInfo *foutcache;
    bool first_arg = true;
    int i;

    // Handle VARIADIC array case - delegate to array_to_text
    if (get_fn_expr_variadic(fcinfo->flinfo))
    {
        ArrayType *arr;

        // Should have just one argument for VARIADIC case
        Assert(argidx == PG_NARGS() - 1);

        if (PG_ARGISNULL(argidx))
            return NULL;

        // Get array and process with array_to_text_internal
        arr = PG_GETARG_ARRAYTYPE_P(argidx);
        return array_to_text_internal(fcinfo, arr, sepstr, NULL);
    }

    // Normal case - concatenate individual arguments
    initStringInfo(&str);

    // Get or build output function cache
    foutcache = (FmgrInfo *) fcinfo->flinfo->fn_extra;
    if (foutcache == NULL)
        foutcache = build_concat_foutcache(fcinfo, argidx);

    // Process each argument starting from argidx
    for (i = argidx; i < PG_NARGS(); i++)
    {
        if (!PG_ARGISNULL(i))
        {
            Datum value = PG_GETARG_DATUM(i);

            // Add separator between values (not before first)
            if (first_arg)
                first_arg = false;
            else
                appendStringInfoString(&str, sepstr);

            // Convert value to string and append
            appendStringInfoString(&str,
                                   OutputFunctionCall(&foutcache[i], value));
        }
    }

    // Convert result to text and clean up
    result = cstring_to_text_with_len(str.data, str.len);
    pfree(str.data);

    return result;
}
```