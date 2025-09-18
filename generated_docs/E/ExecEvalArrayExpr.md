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
  - ArrayGetNItems: Calculate total number of items in array
  - ArrayCheckBounds: Validate array bounds
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