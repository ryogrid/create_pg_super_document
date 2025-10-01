# ExecEvalArrayExpr

## Location
[src/backend/executor/execExprInterp.c:2845-3058](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2845-L3058)

## Overview
ExecEvalArrayExpr evaluates ARRAY[] expressions by constructing array values from pre-computed element values, handling both simple scalar arrays and complex multidimensional nested arrays.

## Definition
```c
void ExecEvalArrayExpr(ExprState *state, ExprEvalStep *op)
```

## Detailed Description
This function constructs PostgreSQL arrays from individual element values that have already been evaluated and stored in op->d.arrayexpr.elemvalues[] and op->d.arrayexpr.elemnulls[]. The function handles two main cases:

1. **Simple arrays** (!multidims): Creates 1-dimensional arrays from scalar elements using construct_md_array()

2. **Multidimensional arrays** (multidims): Constructs higher-dimensional arrays by combining compatible sub-arrays. This involves:
   - Validating that all sub-arrays have compatible dimensions and lower bounds
   - Extracting data and null bitmaps from each sub-array
   - Copying the data into a new multidimensional array structure
   - Handling special cases like empty arrays and null sub-arrays

The function performs extensive validation including type compatibility checks, dimension matching, and memory allocation limits. It follows PostgreSQLs array semantics where multidimensional arrays require all sub-arrays to have identical dimensions and lower bounds.

## Parameters / Member Variables
- `state`: ExprState pointer containing the expression evaluation state
- `op`: ExprEvalStep pointer containing:
  - `op->d.arrayexpr.elemtype`: OID of the array element type
  - `op->d.arrayexpr.nelems`: Number of elements in the array
  - `op->d.arrayexpr.elemvalues[]`: Pre-evaluated element Datum values
  - `op->d.arrayexpr.elemnulls[]`: Null indicators for each element
  - `op->d.arrayexpr.multidims`: Boolean indicating if this is a multidimensional array
  - `op->d.arrayexpr.elemlength`: Length of elements (for simple arrays)
  - `op->d.arrayexpr.elembyval`: Whether elements are passed by value
  - `op->d.arrayexpr.elemalign`: Element alignment requirement
  - `op->resvalue`: Pointer to store the resulting array Datum
  - `op->resnull`: Pointer to store the null indicator (always false)

## Dependencies
- Functions called/Symbols referenced:
  - [construct_md_array](../c/construct_md_array.md): Construct multidimensional array from values
  - [construct_empty_array](../c/construct_empty_array.md): Create an empty array of specified type
  - DatumGetArrayTypeP: Extract ArrayType from Datum
  - [ArrayGetNItems](../A/ArrayGetNItems.md): Calculate total number of items in array
  - [ArrayCheckBounds](../A/ArrayCheckBounds.md): Validate array bounds
  - [array_bitmap_copy](../a/array_bitmap_copy.md): Copy null bitmap between arrays
  - Various ARR_* macros for array access and manipulation
  - Memory allocation functions (palloc, palloc0)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md): Main expression interpreter loop
  - [FunctionReturningBool](../F/FunctionReturningBool.md): JIT compilation type definitions

## Notes and Other Information
- The function always sets *op->resnull to false since array construction never produces NULL
- Extensive error checking prevents incompatible array merging and dimension mismatches
- Memory allocation is carefully managed with overflow checks using AllocSizeIsValid
- Empty and null sub-arrays are handled specially - mixing them with non-empty arrays causes errors
- The function supports arrays up to MAXDIM dimensions
- For multidimensional arrays, null bitmaps from sub-arrays are properly merged
- Performance optimizations include early validation and efficient memory copying

## Simplified Source

```c
void ExecEvalArrayExpr(ExprState *state, ExprEvalStep *op) {
    ArrayType *result;
    Oid element_type = op->d.arrayexpr.elemtype;
    int num_elements = op->d.arrayexpr.nelems;
    int dimensions = 0;
    int dim_sizes[MAXDIM];
    int lower_bounds[MAXDIM];

    // Result is never null
    *op->resnull = false;

    if (!op->d.arrayexpr.multidims) {
        // Simple case: 1D array of scalar values
        Datum *element_values = op->d.arrayexpr.elemvalues;
        bool *element_nulls = op->d.arrayexpr.elemnulls;

        // Set up single dimension
        dimensions = 1;
        dim_sizes[0] = num_elements;
        lower_bounds[0] = 1;

        // Construct the array using PostgreSQL's array constructor
        result = construct_md_array(element_values, element_nulls, dimensions,
                                  dim_sizes, lower_bounds, element_type,
                                  op->d.arrayexpr.elemlength,
                                  op->d.arrayexpr.elembyval,
                                  op->d.arrayexpr.elemalign);
    } else {
        // Complex case: multidimensional array from sub-arrays
        int total_bytes = 0;
        int outer_count = 0;
        int sub_dimensions = 0;
        int *sub_dim_sizes = NULL;
        int *sub_lower_bounds = NULL;
        bool first_subarray = true;
        bool has_nulls = false;
        bool has_empty = false;

        // Arrays to store data from each sub-array
        char **subarray_data = palloc(num_elements * sizeof(char *));
        bits8 **subarray_nullbits = palloc(num_elements * sizeof(bits8 *));
        int *subarray_bytes = palloc(num_elements * sizeof(int));
        int *subarray_items = palloc(num_elements * sizeof(int));

        // Process each sub-array element
        for (int i = 0; i < num_elements; i++) {
            Datum array_datum = op->d.arrayexpr.elemvalues[i];
            bool is_null = op->d.arrayexpr.elemnulls[i];

            // Skip null sub-arrays (handle later)
            if (is_null) {
                has_empty = true;
                continue;
            }

            ArrayType *subarray = DatumGetArrayTypeP(array_datum);

            // Validate element type compatibility
            if (element_type != ARR_ELEMTYPE(subarray))
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                              errmsg("cannot merge incompatible arrays")));

            int this_dimensions = ARR_NDIM(subarray);

            // Skip empty sub-arrays
            if (this_dimensions <= 0) {
                has_empty = true;
                continue;
            }

            if (first_subarray) {
                // Initialize dimensions from first valid sub-array
                sub_dimensions = this_dimensions;
                dimensions = sub_dimensions + 1;

                // Validate total dimensions don't exceed maximum
                if (dimensions <= 0 || dimensions > MAXDIM)
                    ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                                  errmsg("too many array dimensions")));

                sub_dim_sizes = palloc(sub_dimensions * sizeof(int));
                memcpy(sub_dim_sizes, ARR_DIMS(subarray), sub_dimensions * sizeof(int));
                sub_lower_bounds = palloc(sub_dimensions * sizeof(int));
                memcpy(sub_lower_bounds, ARR_LBOUND(subarray), sub_dimensions * sizeof(int));

                first_subarray = false;
            } else {
                // Verify all sub-arrays have matching dimensions
                if (sub_dimensions != this_dimensions ||
                    memcmp(sub_dim_sizes, ARR_DIMS(subarray), sub_dimensions * sizeof(int)) != 0 ||
                    memcmp(sub_lower_bounds, ARR_LBOUND(subarray), sub_dimensions * sizeof(int)) != 0)
                    ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                                  errmsg("multidimensional arrays must have matching dimensions")));
            }

            // Extract data from this sub-array
            subarray_data[outer_count] = ARR_DATA_PTR(subarray);
            subarray_nullbits[outer_count] = ARR_NULLBITMAP(subarray);
            subarray_bytes[outer_count] = ARR_SIZE(subarray) - ARR_DATA_OFFSET(subarray);
            total_bytes += subarray_bytes[outer_count];

            // Check for memory allocation overflow
            if (!AllocSizeIsValid(total_bytes))
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                              errmsg("array size exceeds maximum allowed")));

            subarray_items[outer_count] = ArrayGetNItems(this_dimensions, ARR_DIMS(subarray));
            has_nulls |= ARR_HASNULL(subarray);
            outer_count++;
        }

        // Handle case where all sub-arrays were empty/null
        if (has_empty) {
            if (dimensions == 0) {
                // All were empty - return empty array
                *op->resvalue = PointerGetDatum(construct_empty_array(element_type));
                return;
            }
            // Mixed empty and non-empty is an error
            ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                          errmsg("cannot mix empty and non-empty arrays")));
        }

        // Set up final array dimensions
        dim_sizes[0] = outer_count;
        lower_bounds[0] = 1;
        for (int i = 1; i < dimensions; i++) {
            dim_sizes[i] = sub_dim_sizes[i - 1];
            lower_bounds[i] = sub_lower_bounds[i - 1];
        }

        // Calculate final array structure
        int total_items = ArrayGetNItems(dimensions, dim_sizes);
        ArrayCheckBounds(dimensions, dim_sizes, lower_bounds);

        // Calculate required memory including null bitmap if needed
        int data_offset;
        if (has_nulls) {
            data_offset = ARR_OVERHEAD_WITHNULLS(dimensions, total_items);
            total_bytes += data_offset;
        } else {
            data_offset = 0;
            total_bytes += ARR_OVERHEAD_NONULLS(dimensions);
        }

        // Allocate and initialize result array
        result = (ArrayType *) palloc0(total_bytes);
        SET_VARSIZE(result, total_bytes);
        result->ndim = dimensions;
        result->dataoffset = data_offset;
        result->elemtype = element_type;
        memcpy(ARR_DIMS(result), dim_sizes, dimensions * sizeof(int));
        memcpy(ARR_LBOUND(result), lower_bounds, dimensions * sizeof(int));

        // Copy data from all sub-arrays into result
        char *data_ptr = ARR_DATA_PTR(result);
        int item_index = 0;
        for (int i = 0; i < outer_count; i++) {
            memcpy(data_ptr, subarray_data[i], subarray_bytes[i]);
            data_ptr += subarray_bytes[i];

            // Copy null bitmap if present
            if (has_nulls)
                array_bitmap_copy(ARR_NULLBITMAP(result), item_index,
                                subarray_nullbits[i], 0, subarray_items[i]);
            item_index += subarray_items[i];
        }
    }

    *op->resvalue = PointerGetDatum(result);
}
```