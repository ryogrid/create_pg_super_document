# ReadArrayStr

## Location
[src/backend/utils/adt/arrayfuncs.c:579-795](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L579-L795)

## Overview
Parses array string content enclosed in braces and converts element values to internal format, determining or validating array dimensions during parsing.

## Definition

```c
static bool
ReadArrayStr(char **srcptr,
			 FmgrInfo *inputproc,
			 Oid typioparam,
			 int32 typmod,
			 char typdelim,
			 int typlen,
			 bool typbyval,
			 char typalign,
			 int *ndim_p,
			 int *dim,
			 int *nitems_p,
			 Datum **values_p,
			 bool **nulls_p,
			 const char *origStr,
			 Node *escontext)
```
## Detailed Description
ReadArrayStr is the core parsing function that processes the content within array braces "{ ... }" and converts element values to their internal PostgreSQL representation. It handles both dimension discovery and validation while parsing nested array structures.

The function operates as a state machine that tracks nesting levels, element counts, and delimiter expectations. It can work in two modes:
1. **Dimension discovery mode**: When ndim_p is 0, it determines dimensions from array structure
2. **Dimension validation mode**: When dimensions are pre-specified, it validates the structure matches

Key parsing behaviors:
- Uses ReadArrayToken to tokenize the input stream
- Maintains element counting at each nesting level to validate consistent dimensions
- Dynamically resizes value and null arrays as needed
- Calls element type input functions to convert string values to Datums
- Enforces array size limits (MaxArraySize) to prevent excessive memory usage
- Validates that multi-dimensional arrays have consistent sub-array lengths

The function handles NULL values explicitly through ATOK_ELEM_NULL tokens and maintains separate arrays for values and null indicators.

## Parameters / Member Variables
- `**srcptr`: Pointer to current position in input string, advanced during parsing
- `*inputproc`: FmgrInfo for element type's input conversion function
- `typioparam`: Additional parameter for element input function
- `typmod`: Type modifier for element type
- `typdelim`: Delimiter character for array elements (type-specific)
- `typlen`: Storage length of element type
- `typbyval`: Whether element type is passed by value
- `typalign`: Alignment requirement for element type
- `*ndim_p`: Input/output parameter for number of dimensions
- `*dim`: Input/output array for dimension sizes
- `*nitems_p`: Output parameter for total number of elements parsed
- `**values_p`: Output parameter for array of parsed element values
- `**nulls_p`: Output parameter for array of null indicators
- `*origStr`: Original input string (used only for error messages)
- `*escontext`: Error context for soft error handling
## Dependencies
- Functions called/Symbols referenced:
  - [ReadArrayToken](ReadArrayToken.md)
  - palloc_array
  - repalloc_array
  - [InputFunctionCallSafe](../I/InputFunctionCallSafe.md)
  - [initStringInfo](../i/initStringInfo.md)
  - MaxArraySize
  - MAXDIM
  - ArrayToken types (ATOK_LEVEL_START, ATOK_LEVEL_END, etc.)
- Called from (representative examples):
  - [array_in](../a/array_in.md)

## Notes and Other Information
- Static function internal to arrayfuncs.c
- Expects srcptr to point to opening '{' and advances it past closing '}'
- Maintains strict validation of array structure and consistent dimensions
- Uses dynamic memory allocation and reallocation for optimal performance
- Implements comprehensive error checking with detailed error messages
- Handles arrays up to MAXDIM dimensions
- Supports both explicit NULL values and empty string elements
- Freezes dimensionality once first element is encountered to ensure consistency

## Simplified Source

```c
static bool
ReadArrayStr(char **srcptr, FmgrInfo *inputproc, Oid typioparam, int32 typmod,
             char typdelim, int typlen, bool typbyval, char typalign,
             int *ndim_p, int *dim, int *nitems_p,
             Datum **values_p, bool **nulls_p,
             const char *origStr, Node *escontext)
{
    int ndim = *ndim_p;
    bool dimensions_specified = (ndim != 0);
    int maxitems = 16;
    Datum *values = palloc_array(Datum, maxitems);
    bool *nulls = palloc_array(bool, maxitems);
    StringInfoData elembuf;
    int nest_level = 0;
    int nitems = 0;
    bool ndim_frozen = dimensions_specified;
    bool expect_delim = false;
    int nelems[MAXDIM];

    // Initialize element buffer for string parsing
    initStringInfo(&elembuf);

    // Main parsing loop - process tokens until matching right brace
    do {
        ArrayToken tok = ReadArrayToken(srcptr, &elembuf, typdelim, origStr, escontext);

        switch (tok) {
            case ATOK_LEVEL_START:  // '{'
                // Start new nesting level
                if (expect_delim || nest_level >= MAXDIM)
                    return false;  // Error handling simplified

                nelems[nest_level] = 0;
                nest_level++;
                if (nest_level > ndim) {
                    if (ndim_frozen) goto dimension_error;
                    ndim = nest_level;
                }
                break;

            case ATOK_LEVEL_END:    // '}'
                // End current nesting level
                if (nelems[nest_level - 1] > 0 && !expect_delim)
                    return false;

                nest_level--;
                if (nest_level > 0)
                    nelems[nest_level - 1]++;

                // Validate sub-array dimensions
                if (dim[nest_level] < 0) {
                    dim[nest_level] = nelems[nest_level];
                } else if (nelems[nest_level] != dim[nest_level]) {
                    goto dimension_error;
                }
                expect_delim = true;
                break;

            case ATOK_DELIM:        // delimiter character
                if (!expect_delim) return false;
                expect_delim = false;
                break;

            case ATOK_ELEM:         // actual element value
            case ATOK_ELEM_NULL:    // NULL element
                if (expect_delim) return false;

                // Expand arrays if needed
                if (nitems >= maxitems) {
                    if (maxitems >= MaxArraySize) return false;
                    maxitems = Min(maxitems * 2, MaxArraySize);
                    values = repalloc_array(values, Datum, maxitems);
                    nulls = repalloc_array(nulls, bool, maxitems);
                }

                // Convert element value using type input function
                if (!InputFunctionCallSafe(inputproc,
                                         (tok == ATOK_ELEM_NULL) ? NULL : elembuf.data,
                                         typioparam, typmod, escontext,
                                         &values[nitems]))
                    return false;

                nulls[nitems] = (tok == ATOK_ELEM_NULL);
                nitems++;

                // Lock dimensions after first element
                ndim_frozen = true;
                if (nest_level != ndim) goto dimension_error;
                nelems[nest_level - 1]++;
                expect_delim = true;
                break;

            case ATOK_ERROR:
                return false;
        }
    } while (nest_level > 0);

    // Clean up and return results
    pfree(elembuf.data);
    *ndim_p = ndim;
    *nitems_p = nitems;
    *values_p = values;
    *nulls_p = nulls;
    return true;

dimension_error:
    // Dimension validation failed
    return false;
}
```