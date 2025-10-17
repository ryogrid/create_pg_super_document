# ReadArrayDimensions

## Location
[src/backend/utils/adt/arrayfuncs.c:402-518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L402-L518)

## Overview
Parses array dimension specifications from an input string and converts them to internal format, extracting bounds and dimension sizes for multi-dimensional arrays.

## Definition

```c
static bool
ReadArrayDimensions(char **srcptr, int *ndim_p, int *dim, int *lBound,
					const char *origStr, Node *escontext)
```
## Detailed Description
ReadArrayDimensions is a static helper function that parses the optional dimension specification part of PostgreSQL array literals. It handles dimension specifications in the format "[n]" for simple dimensions or "[m:n]" for explicit lower and upper bounds.

The function processes dimension items sequentially, validating bounds and computing dimension sizes. It supports multi-dimensional arrays up to MAXDIM dimensions. The parsing follows these rules:
- Dimension items are enclosed in square brackets: [n] or [m:n]
- Whitespace is allowed between dimension items but not within them
- For [n] format, lower bound defaults to 1 and upper bound is n
- For [m:n] format, lower bound is m and upper bound is n
- Upper bound cannot be less than lower bound
- Upper bound cannot be INT_MAX (reserved for internal use)

The function performs careful overflow checking when computing dimension sizes to prevent integer overflow attacks.

## Parameters / Member Variables
- `**srcptr`: Pointer to current position in input string, advanced during parsing
- `*ndim_p`: Output parameter for number of dimensions found
- `*dim`: Output array for dimension sizes (caller-allocated, MAXDIM elements)
- `*lBound`: Output array for lower bounds of each dimension (caller-allocated, MAXDIM elements)
- `*origStr`: Original input string (used only for error messages)
- `*escontext`: Error context for soft error handling
## Dependencies
- Functions called/Symbols referenced:
  - [ReadDimensionInt](ReadDimensionInt.md)
  - [scanner_isspace](../s/scanner_isspace.md)
  - [pg_sub_s32_overflow](../p/pg_sub_s32_overflow.md)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md)
  - ereturn (error handling macros)
  - MAXDIM
  - MaxArraySize
- Called from (representative examples):
  - [array_in](../a/array_in.md)

## Notes and Other Information
- Static function internal to arrayfuncs.c
- Advances the source pointer (*srcptr) to the position after parsed dimensions
- Sets *ndim_p to 0 if no dimension specifications are found
- Validates that dimensions don't exceed MAXDIM limit
- Performs overflow checking to prevent arithmetic overflow in dimension calculations
- Does not accept zero-length dimensions (where upper bound < lower bound)
- Uses soft error handling through escontext when available

## Simplified Source

```c
static bool
ReadArrayDimensions(char **srcptr, int *ndim_p, int *dim, int *lBound,
                    const char *origStr, Node *escontext)
{
    char   *p = *srcptr;
    int     ndim = 0;

    // Parse dimension items in format [n] or [m:n]
    for (;;) {
        char   *q;
        int     i, ub;

        // Skip whitespace between dimension items
        while (scanner_isspace(*p))
            p++;

        // No more dimensions if not starting with '['
        if (*p != '[')
            break;
        p++;

        // Check dimension limit
        if (ndim >= MAXDIM)
            ereturn(escontext, false, /* error: too many dimensions */);

        // Parse first integer (lower bound or single dimension)
        q = p;
        if (!ReadDimensionInt(&p, &i, origStr, escontext))
            return false;
        if (p == q)  // no digits found
            ereturn(escontext, false, /* error: missing dimension value */);

        if (*p == ':') {
            // [m:n] format - explicit bounds
            lBound[ndim] = i;
            p++;
            q = p;
            if (!ReadDimensionInt(&p, &ub, origStr, escontext))
                return false;
            if (p == q)  // no digits after ':'
                ereturn(escontext, false, /* error: missing upper bound */);
        } else {
            // [n] format - dimension size with default lower bound
            lBound[ndim] = 1;
            ub = i;
        }

        // Expect closing bracket
        if (*p != ']')
            ereturn(escontext, false, /* error: missing ']' */);
        p++;

        // Validate bounds
        if (ub < lBound[ndim])
            ereturn(escontext, false, /* error: upper < lower bound */);

        if (ub == INT_MAX)
            ereturn(escontext, false, /* error: upper bound too large */);

        // Calculate dimension size with overflow checking
        if (pg_sub_s32_overflow(ub, lBound[ndim], &ub) ||
            pg_add_s32_overflow(ub, 1, &ub))
            ereturn(escontext, false, /* error: dimension size overflow */);

        dim[ndim] = ub;
        ndim++;
    }

    *srcptr = p;
    *ndim_p = ndim;
    return true;
}
```