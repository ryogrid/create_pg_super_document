# array_to_text_internal

## Location
[src/backend/utils/adt/varlena.c:4808-4929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4808-L4929)

## Overview
Core internal function that implements the array-to-text conversion logic shared by both array_to_text and array_to_text_null functions, handling element iteration, NULL value processing, and output formatting.

## Definition

```c
static text *
array_to_text_internal(FunctionCallInfo fcinfo, ArrayType *v,
					   const char *fldsep, const char *null_string)
```
## Detailed Description
This function performs the actual work of converting a PostgreSQL array to a concatenated text string. It handles multi-dimensional arrays by flattening them and processes each element through the appropriate output function for the element type. The function implements sophisticated caching of type metadata to avoid repeated lookups when processing multiple arrays of the same element type. It correctly handles NULL elements based on whether a null replacement string is provided, and manages proper memory alignment when traversing variable-length array elements. The function uses PostgreSQL's StringInfo buffer for efficient string concatenation and properly handles the null bitmap for sparse arrays.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing context and caching capabilities
- `*v`: Pointer to the ArrayType structure representing the input array
- `*fldsep`: C string containing the field separator to use between array elements
- `*null_string`: Optional C string to substitute for NULL array elements (NULL means skip NULLs)
## Dependencies
- Functions called/Symbols referenced:
  - ARR_NDIM, ARR_DIMS, ARR_ELEMTYPE (array metadata access macros)
  - [ArrayGetNItems](../A/ArrayGetNItems.md) (calculates total number of elements)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md) (converts C string to PostgreSQL text type)
  - [get_type_io_data](../g/get_type_io_data.md) (retrieves type metadata and output function)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (caches function call information)
  - ARR_DATA_PTR, ARR_NULLBITMAP (array data access macros)
  - [fetch_att](../f/fetch_att.md) (extracts individual array elements with proper alignment)
  - [OutputFunctionCall](../O/OutputFunctionCall.md) (converts elements to string representation)
  - att_addlength_pointer, att_align_nominal (memory alignment utilities)
  - [initStringInfo](../i/initStringInfo.md), appendStringInfo, appendStringInfoString (string buffer operations)
- Called from:
  - [array_to_text](array_to_text.md) (two-parameter version)
  - [array_to_text_null](array_to_text_null.md) (three-parameter version)
  - [concat_internal](../c/concat_internal.md) (used in string concatenation operations)

## Notes and Other Information
The function employs a sophisticated caching mechanism using ArrayMetaState stored in the function's extra space to avoid repeated type lookups when the same function is called multiple times with arrays of the same element type. This optimization is particularly important for performance in loops or bulk operations. The function handles both fixed-length and variable-length element types correctly, managing memory alignment requirements for each. The null bitmap processing allows efficient handling of sparse arrays where not all positions contain values. The function is marked static as it's an internal implementation detail shared between the public array_to_text functions.

## Simplified Source

```c
static text *array_to_text_internal(FunctionCallInfo fcinfo, ArrayType *v,
                                   const char *fldsep, const char *null_string) {
    // Get array metadata
    int ndims = ARR_NDIM(v);
    int *dims = ARR_DIMS(v);
    int nitems = ArrayGetNItems(ndims, dims);

    // Handle empty array
    if (nitems == 0)
        return cstring_to_text_with_len("", 0);

    Oid element_type = ARR_ELEMTYPE(v);
    StringInfoData buf;
    initStringInfo(&buf);

    // Setup or reuse cached type metadata for performance
    ArrayMetaState *my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
    if (my_extra == NULL) {
        fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
                                                      sizeof(ArrayMetaState));
        my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
        my_extra->element_type = ~element_type;  // Force refresh
    }

    // Refresh type info if element type changed
    if (my_extra->element_type != element_type) {
        get_type_io_data(element_type, IOFunc_output,
                        &my_extra->typlen, &my_extra->typbyval,
                        &my_extra->typalign, &my_extra->typdelim,
                        &my_extra->typioparam, &my_extra->typiofunc);
        fmgr_info_cxt(my_extra->typiofunc, &my_extra->proc,
                     fcinfo->flinfo->fn_mcxt);
        my_extra->element_type = element_type;
    }

    // Setup for array traversal
    char *p = ARR_DATA_PTR(v);
    bits8 *bitmap = ARR_NULLBITMAP(v);
    int bitmask = 1;
    bool printed = false;

    // Process each array element
    for (int i = 0; i < nitems; i++) {
        // Check if element is NULL
        if (bitmap && (*bitmap & bitmask) == 0) {
            // Handle NULL element
            if (null_string != NULL) {
                if (printed)
                    appendStringInfo(&buf, "%s%s", fldsep, null_string);
                else
                    appendStringInfoString(&buf, null_string);
                printed = true;
            }
        } else {
            // Extract and convert non-NULL element
            Datum itemvalue = fetch_att(p, my_extra->typbyval, my_extra->typlen);
            char *value = OutputFunctionCall(&my_extra->proc, itemvalue);

            // Add separator if not first element
            if (printed)
                appendStringInfo(&buf, "%s%s", fldsep, value);
            else
                appendStringInfoString(&buf, value);
            printed = true;

            // Move to next element with proper alignment
            p = att_addlength_pointer(p, my_extra->typlen, p);
            p = (char *) att_align_nominal(p, my_extra->typalign);
        }

        // Advance null bitmap tracking
        if (bitmap) {
            bitmask <<= 1;
            if (bitmask == 0x100) {
                bitmap++;
                bitmask = 1;
            }
        }
    }

    // Convert result buffer to text and clean up
    text *result = cstring_to_text_with_len(buf.data, buf.len);
    pfree(buf.data);

    return result;
}
```