# populate_array_dim_jsonb

## Location
[src/backend/utils/adt/jsonfuncs.c:2823-2912](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2823-L2912)

## Overview
Recursively iterates through JSONB sub-array elements to populate a PostgreSQL array structure, handling multi-dimensional arrays with proper dimension validation.

## Definition

```c
static bool
populate_array_dim_jsonb(PopulateArrayContext *ctx, /* context */
						 JsonbValue *jbv,	/* jsonb sub-array */
						 int ndim)	/* current dimension */
```
## Detailed Description
This function performs recursive traversal of JSONB array structures to populate PostgreSQL arrays. It uses a JSONB iterator to systematically process array elements, handling both the determination of array dimensions and the population of array elements. The function validates that the input JSONB value represents an array and manages the recursive descent through nested array structures. It coordinates with the array dimension assignment system to determine the total number of dimensions when not yet known, and validates dimensional consistency as it processes elements. The function handles both leaf-level element processing and recursive calls for nested sub-arrays.

## Parameters / Member Variables
- `*ctx`: PopulateArrayContext pointer containing array metadata, dimension information, and error context
- `*jbv`: JsonbValue pointer representing the JSONB sub-array to be processed
- `ndim`: Integer representing the current dimension level being processed
## Dependencies
- Functions called/Symbols referenced:
  - [PopulateArrayContext](../P/PopulateArrayContext.md) (context structure)
  - [JsonbContainer](../J/JsonbContainer.md) (JSONB container structure) 
  - JsonbIterator (JSONB iterator structure)
  - JsonbIteratorToken (iterator token type)
  - [JsValue](../J/JsValue.md) (value representation structure)
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - JsonContainerIsArray (array type check)
  - jbvBinary (JSONB binary type constant)
  - JsonContainerIsScalar (scalar type check)
  - [populate_array_report_expected_array](populate_array_report_expected_array.md) (error reporting)
  - SOFT_ERROR_OCCURRED (error checking macro)
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md) (iterator initialization)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md) (iterator advancement)
  - WJB_BEGIN_ARRAY, WJB_END_ARRAY, WJB_ELEM, WJB_DONE (iterator token constants)
  - [populate_array_assign_ndims](populate_array_assign_ndims.md) (dimension assignment)
  - [populate_array_element](populate_array_element.md) (element processing)
  - [populate_array_check_dimension](populate_array_check_dimension.md) (dimension validation)
- Called from (representative examples):
  - JsObjectFree
  - [populate_array_dim_jsonb](populate_array_dim_jsonb.md) (recursive self-call)
  - [populate_array](populate_array.md)

## Notes and Other Information
- This is a static function within jsonfuncs.c serving as an internal implementation detail
- Uses recursive design to handle arbitrarily nested array structures
- Includes stack depth checking to prevent stack overflow on deeply nested arrays
- The function can handle scalars that arrive via ExecEvalJsonCoercion() by reporting appropriate errors
- Manages JSONB iterator lifecycle, ensuring proper cleanup by iterating to WJB_DONE
- Returns false on any error condition, supporting PostgreSQL's error-safe processing paradigm
- Coordinates closely with populate_array_assign_ndims() for dynamic dimension discovery
- Part of PostgreSQL's JSONB-to-array conversion infrastructure, supporting complex multi-dimensional array structures

## Simplified Source

```c
static bool populate_array_dim_jsonb(PopulateArrayContext *ctx, JsonbValue *jbv, int ndim)
{
    JsonbContainer *jbc = jbv->val.binary.data;
    JsonbIterator *it;
    JsonbIteratorToken tok;
    JsonbValue val;
    JsValue jsv;

    check_stack_depth();

    // Validate input is a JSONB array (not scalar or other type)
    if (jbv->type != jbvBinary || !JsonContainerIsArray(jbc) || JsonContainerIsScalar(jbc))
    {
        populate_array_report_expected_array(ctx, ndim - 1);
        return false;  // Error reported softly
    }

    // Initialize JSONB iterator and start processing
    it = JsonbIteratorInit(jbc);
    tok = JsonbIteratorNext(&it, &val, true);  // WJB_BEGIN_ARRAY
    tok = JsonbIteratorNext(&it, &val, true);  // First element or WJB_END_ARRAY

    // Determine array dimensions if not yet known
    if (ctx->ndims <= 0 &&
        (tok == WJB_END_ARRAY ||
         (tok == WJB_ELEM && (val.type != jbvBinary || !JsonContainerIsArray(val.val.binary.data)))))
    {
        if (!populate_array_assign_ndims(ctx, ndim))
            return false;
    }

    jsv.is_json = false;
    jsv.val.jsonb = &val;

    // Process all array elements
    while (tok == WJB_ELEM)
    {
        if (ctx->ndims > 0 && ndim >= ctx->ndims)
        {
            // Process leaf element
            if (!populate_array_element(ctx, ndim, &jsv))
                return false;
        }
        else
        {
            // Recursively process sub-array
            if (!populate_array_dim_jsonb(ctx, &val, ndim + 1))
                return false;

            // Validate dimension consistency
            if (!populate_array_check_dimension(ctx, ndim))
                return false;
        }

        tok = JsonbIteratorNext(&it, &val, true);
    }

    // Clean up iterator (iterate until WJB_DONE)
    tok = JsonbIteratorNext(&it, &val, true);

    return true;
}
```