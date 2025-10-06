# compute_bucket

## Location
[src/backend/utils/adt/numeric.c:1933-2020](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1933-L2020)

## Overview
Calculates the bucket number for histogram bucket operations by determining which bucket a numeric operand falls into within a specified range, following SQL2003 specification.

## Definition

```c
struct
		 */
		nss->buf = palloc(VARATT_SHORT_MAX + VARHDRSZ + 1);
```
## Detailed Description
The  function is a core computational component for PostgreSQL's  functionality. It performs the mathematical operations to determine which histogram bucket a given numeric value should be placed in. The function implements the bucket calculation algorithm specified in the SQL2003 standard.

The function handles both normal and reversed bound scenarios (where bound1 > bound2). It performs precise numeric arithmetic by converting input Numeric values to NumericVar format for computation, then calculating the proportional position of the operand within the range defined by the bounds. The result is multiplied by the bucket count and divided by the range width to determine the bucket number.

To avoid roundoff errors, the function multiplies by count before dividing. It also includes safeguards to ensure the result doesn't exceed the maximum bucket count due to floating-point precision issues.

## Parameters / Member Variables
- : The numeric value for which to determine the bucket
- : The first boundary value of the range
- : The second boundary value of the range  
- : The total number of buckets as a NumericVar
- : Boolean flag indicating if bound1 > bound2
- : Output parameter to store the calculated bucket number

## Dependencies
- Functions called/Symbols referenced:
  - [init_var_from_num](../i/init_var_from_num.md)
  - [sub_var](../s/sub_var.md)
  - [mul_var](../m/mul_var.md)
  - [div_var](../d/div_var.md)
  - [select_div_scale](../s/select_div_scale.md)
  - [cmp_var](cmp_var.md)
  - [set_var_from_var](../s/set_var_from_var.md)
  - [add_var](../a/add_var.md)
  - [floor_var](../f/floor_var.md)
  - [free_var](../f/free_var.md)
- Called from (representative examples):
  - [width_bucket_numeric](../w/width_bucket_numeric.md)

## Notes and Other Information
- This is a static function internal to the numeric.c module
- Implements SQL2003 width_bucket specification
- Includes precision safeguards to handle floating-point roundoff errors
- Memory management is handled through free_var() calls for temporary variables
- The function assumes the operand is within the valid bucket range

## Simplified Source

```c
static void
compute_bucket(Numeric operand, Numeric bound1, Numeric bound2,
               const NumericVar *count_var, bool reversed_bounds,
               NumericVar *result_var)
{
    NumericVar bound1_var;
    NumericVar bound2_var;
    NumericVar operand_var;

    // Convert inputs to NumericVar format
    init_var_from_num(bound1, &bound1_var);
    init_var_from_num(bound2, &bound2_var);
    init_var_from_num(operand, &operand_var);

    // Calculate relative position based on bound direction
    if (!reversed_bounds)
    {
        // Normal case: bound1 < bound2
        sub_var(&operand_var, &bound1_var, &operand_var);
        sub_var(&bound2_var, &bound1_var, &bound2_var);
    }
    else
    {
        // Reversed case: bound1 > bound2
        sub_var(&bound1_var, &operand_var, &operand_var);
        sub_var(&bound1_var, &bound2_var, &bound2_var);
    }

    // Multiply by count before dividing to avoid roundoff errors
    mul_var(&operand_var, count_var, &operand_var,
            operand_var.dscale + count_var->dscale);
    div_var(&operand_var, &bound2_var, result_var,
            select_div_scale(&operand_var, &bound2_var), true);

    // Clamp result to avoid exceeding count due to roundoff
    if (cmp_var(result_var, count_var) >= 0)
        set_var_from_var(count_var, result_var);
    else
    {
        add_var(result_var, &const_one, result_var);
        floor_var(result_var, result_var);
    }

    // Clean up temporary variables
    free_var(&bound1_var);
    free_var(&bound2_var);
    free_var(&operand_var);
}
```