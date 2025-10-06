# accum_sum_final

## Location
[src/backend/utils/adt/numeric.c:12202-12252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L12202-L12252)

## Overview
Finalizes the accumulator computation by performing final carry propagation and converting the accumulated positive and negative sums into a single NumericVar result.

## Definition

```c
static void
accum_sum_final(NumericSumAccum *accum, NumericVar *result)
```
## Detailed Description
This function completes the accumulation process by first performing any pending carry propagation, then creating separate NumericVar structures for the positive and negative accumulated values, and finally combining them through subtraction to produce the final result. The function handles the conversion from the internal int32 digit representation used in the accumulator to the int16 NumericDigit representation used in NumericVar structures. It also strips leading and trailing zeros from the final result to maintain canonical numeric representation.

The function is designed to work across memory contexts, allowing callers to be in different memory contexts than the accumulator itself.

## Parameters / Member Variables
- `*accum`: Pointer to the NumericSumAccum structure containing the accumulated values
- `*result`: Pointer to the NumericVar where the final computed result will be stored
## Dependencies
- Functions called/Symbols referenced:
  - [accum_sum_carry](accum_sum_carry.md)
  - [set_var_from_var](../s/set_var_from_var.md)
  - init_var
  - digitbuf_alloc
  - [add_var](add_var.md)
  - [strip_var](../s/strip_var.md)
  - [NumericSumAccum](../N/NumericSumAccum.md)
  - [NumericVar](../N/NumericVar.md)
  - NUMERIC_POS
  - NUMERIC_NEG
  - NBASE
  - const_zero
- Called from (representative examples):
  - [numeric_avg_serialize](../n/numeric_avg_serialize.md)
  - [numeric_serialize](../n/numeric_serialize.md)
  - [numeric_poly_serialize](../n/numeric_poly_serialize.md)
  - [int8_avg_serialize](../i/int8_avg_serialize.md)
  - [numeric_avg](../n/numeric_avg.md)
  - [numeric_sum](../n/numeric_sum.md)
  - [numeric_stddev_internal](../n/numeric_stddev_internal.md)
  - [accum_sum_combine](accum_sum_combine.md)

## Notes and Other Information
- The function returns zero (via const_zero) if the accumulator has no digits (ndigits == 0)
- Final carry propagation is performed to ensure all digits are in proper NBASE range before conversion
- Separate NumericVar structures are created for positive and negative sums with identical weight, scale, and digit count
- The int32 accumulator digits are safely converted to int16 NumericDigit format with assertions to verify they're within NBASE
- The final result is the sum of the positive NumericVar and negative NumericVar (which effectively performs subtraction due to the negative sign)
- Leading and trailing zeros are stripped to maintain canonical numeric representation
- Unlike other accumulator functions, this one doesn't require the caller to be in the accumulator's memory context

## Simplified Source

```c
static void accum_sum_final(NumericSumAccum *accum, NumericVar *result) {
    // Return zero if no accumulated digits
    if (accum->ndigits == 0) {
        set_var_from_var(&const_zero, result);
        return;
    }

    // Perform final carry propagation
    accum_sum_carry(accum);

    // Create NumericVars for positive and negative sums
    NumericVar pos_var, neg_var;
    init_var(&pos_var);
    init_var(&neg_var);

    // Set common properties
    pos_var.ndigits = neg_var.ndigits = accum->ndigits;
    pos_var.weight = neg_var.weight = accum->weight;
    pos_var.dscale = neg_var.dscale = accum->dscale;
    pos_var.sign = NUMERIC_POS;
    neg_var.sign = NUMERIC_NEG;

    // Allocate digit buffers
    pos_var.buf = pos_var.digits = digitbuf_alloc(accum->ndigits);
    neg_var.buf = neg_var.digits = digitbuf_alloc(accum->ndigits);

    // Convert int32 accumulator digits to int16 NumericDigits
    for (int i = 0; i < accum->ndigits; i++) {
        pos_var.digits[i] = (int16) accum->pos_digits[i];
        neg_var.digits[i] = (int16) accum->neg_digits[i];
    }

    // Combine positive and negative sums
    add_var(&pos_var, &neg_var, result);

    // Clean up result
    strip_var(result);
}
```