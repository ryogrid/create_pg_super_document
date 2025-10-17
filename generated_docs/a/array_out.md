# array_out

## Location
[src/backend/utils/adt/arrayfuncs.c:1016-1200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L1016-L1200)

## Overview
Converts the internal representation of a PostgreSQL array to its external string format for output and display purposes.

## Definition

```c
struct the output string */
	retval = (char *) palloc(overall_length);
```
## Detailed Description
array_out is the primary output function for PostgreSQL arrays, responsible for converting internal ArrayType structures into their textual representation. The function handles multi-dimensional arrays, properly formats null values, manages element quoting requirements, and includes explicit dimension bounds when necessary. It uses a caching mechanism (ArrayMetaState) to avoid repeated lookups of element type information across function calls.

The function performs several key operations: determines if explicit dimension bounds are needed (when lower bounds aren't 1), converts each array element to string format using the element type's output function, applies proper quoting and escaping for special characters, and constructs the final string with appropriate braces and delimiters. The output format follows PostgreSQL's standard array syntax with curly braces and comma separation.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ANY_ARRAY_P
  - AARR_ELEMTYPE
  - AARR_NDIM
  - AARR_DIMS
  - AARR_LBOUND
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [get_type_io_data](../g/get_type_io_data.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [array_iter_setup](array_iter_setup.md)
  - [array_iter_next](array_iter_next.md)
  - [OutputFunctionCall](../O/OutputFunctionCall.md)
  - [scanner_isspace](../s/scanner_isspace.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
- Called from (representative examples):
  - [anyarray_out](anyarray_out.md)
  - [anycompatiblearray_out](anycompatiblearray_out.md)
  - [CATALOG](../C/CATALOG.md) (pg_type.h)

## Notes and Other Information
The function uses an ArrayMetaState cache structure to store element type information, avoiding repeated type lookups for better performance. It handles special cases like empty strings and literal "NULL" values by forcing quotes. The function calculates exact memory requirements before constructing the output string to avoid buffer overflows. Multi-dimensional arrays include explicit dimension bounds (e.g., [1:3][1:2]) when lower bounds differ from 1. Character escaping follows PostgreSQL standards, with backslashes and double quotes being escaped with backslashes.

## Simplified Source

```c
Datum
array_out(PG_FUNCTION_ARGS)
{
    AnyArrayType *v = PG_GETARG_ANY_ARRAY_P(0);
    Oid element_type = AARR_ELEMTYPE(v);
    int typlen, ndim, nitems, i;
    bool typbyval;
    char typalign, typdelim;
    char *retval, **values;
    bool *needquotes, needdims = false;
    size_t overall_length;
    int *dims, *lb;
    array_iter iter;
    ArrayMetaState *my_extra;

    // Cache element type information for performance
    my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
    if (my_extra == NULL || my_extra->element_type != element_type) {
        if (my_extra == NULL) {
            fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
                                                         sizeof(ArrayMetaState));
            my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
        }

        // Get element type I/O information
        get_type_io_data(element_type, IOFunc_output,
                        &my_extra->typlen, &my_extra->typbyval,
                        &my_extra->typalign, &my_extra->typdelim,
                        &my_extra->typioparam, &my_extra->typiofunc);
        fmgr_info_cxt(my_extra->typiofunc, &my_extra->proc,
                     fcinfo->flinfo->fn_mcxt);
        my_extra->element_type = element_type;
    }

    // Extract array metadata
    typlen = my_extra->typlen;
    typbyval = my_extra->typbyval;
    typalign = my_extra->typalign;
    typdelim = my_extra->typdelim;
    ndim = AARR_NDIM(v);
    dims = AARR_DIMS(v);
    lb = AARR_LBOUND(v);
    nitems = ArrayGetNItems(ndim, dims);

    // Handle empty array
    if (nitems == 0) {
        retval = pstrdup("{}");
        PG_RETURN_CSTRING(retval);
    }

    // Check if explicit dimensions needed (non-1 lower bounds)
    for (i = 0; i < ndim; i++) {
        if (lb[i] != 1) {
            needdims = true;
            break;
        }
    }

    // Convert all elements to strings and determine quoting needs
    values = (char **) palloc(nitems * sizeof(char *));
    needquotes = (bool *) palloc(nitems * sizeof(bool));
    overall_length = 0;

    array_iter_setup(&iter, v);

    for (i = 0; i < nitems; i++) {
        Datum itemvalue;
        bool isnull, needquote;

        // Get array element
        itemvalue = array_iter_next(&iter, &isnull, i,
                                   typlen, typbyval, typalign);

        if (isnull) {
            values[i] = pstrdup("NULL");
            overall_length += 4;
            needquote = false;
        } else {
            // Convert element to string
            values[i] = OutputFunctionCall(&my_extra->proc, itemvalue);

            // Determine if quotes needed and count escaped characters
            needquote = (values[i][0] == '\0' ||  // empty string
                        pg_strcasecmp(values[i], "NULL") == 0);  // literal NULL

            for (char *tmp = values[i]; *tmp != '\0'; tmp++) {
                char ch = *tmp;
                overall_length += 1;
                if (ch == '"' || ch == '\\') {
                    needquote = true;
                    overall_length += 1;  // escape character
                } else if (ch == '{' || ch == '}' || ch == typdelim ||
                          scanner_isspace(ch)) {
                    needquote = true;
                }
            }
        }

        needquotes[i] = needquote;
        if (needquote) overall_length += 2;  // quotes
        overall_length += 1;  // delimiter
    }

    // Calculate space for braces and dimension bounds
    for (i = 0, int j = 0, k = 1; i < ndim; i++) {
        j += k, k *= dims[i];
    }
    overall_length += 2 * j;

    // Add explicit dimensions if needed
    char dims_str[(MAXDIM * 33) + 2];
    dims_str[0] = '\0';
    if (needdims) {
        char *ptr = dims_str;
        for (i = 0; i < ndim; i++) {
            sprintf(ptr, "[%d:%d]", lb[i], lb[i] + dims[i] - 1);
            ptr += strlen(ptr);
        }
        *ptr++ = *ASSGN;  // assignment operator
        *ptr = '\0';
        overall_length += ptr - dims_str;
    }

    // Build the output string with proper formatting
    retval = (char *) palloc(overall_length);
    // [Actual string construction logic would continue here...]

    PG_RETURN_CSTRING(retval);
}
```