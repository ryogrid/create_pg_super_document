# accum_sum_rescale

## Location
[src/backend/utils/adt/numeric.c:12113-12201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L12113-L12201)

## Overview
Adjusts the scale and capacity of a NumericSumAccum structure to accommodate a new numeric value that may have different weight, precision, or scale requirements.

## Definition

```c
static void
accum_sum_rescale(NumericSumAccum *accum, const NumericVar *val)
```
## Detailed Description
This function dynamically resizes and rescales the accumulator's digit buffers when a new value needs to be added that doesn't fit within the current capacity or scale. It handles three main scenarios: enlarging buffers when the new value has a larger weight (more significant digits), ensuring carry space is available after previous carry propagations, and extending precision when the new value has more digits to the right of the decimal point. The function maintains the invariant that the accumulator always has one extra digit of weight beyond what's needed for the inputs, providing space for carry propagation.

When resizing is needed, the function allocates new buffers and copies existing data to the appropriate positions, ensuring that the accumulated values are preserved correctly.

## Parameters / Member Variables
- `*accum`: Pointer to the NumericSumAccum structure to be rescaled
- `*val`: Pointer to the NumericVar containing the new value that determines rescaling requirements
## Dependencies
- Functions called/Symbols referenced:
  - [NumericSumAccum](../N/NumericSumAccum.md)
  - [NumericVar](../N/NumericVar.md)
  - [palloc0](../p/palloc0.md)
  - memcpy
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [accum_sum_add](accum_sum_add.md)

## Notes and Other Information
- The function maintains that the accumulator weight is always one larger than needed for inputs to provide carry space
- Both positive and negative digit buffers are resized simultaneously to maintain consistency
- Memory management is handled properly with palloc0/pfree for dynamic buffer allocation
- The function updates both the weight (position of most significant digit) and ndigits (total digit count)
- Scale (dscale) is updated to the maximum of the current and new value's scale
- After rescaling, have_carry_space is set to true since new buffers have reserved space
- The function only performs actual reallocation when necessary, optimizing performance for cases where rescaling isn't needed

## Simplified Source

```c
static void accum_sum_rescale(NumericSumAccum *accum, const NumericVar *val) {
    int old_weight = accum->weight;
    int old_ndigits = accum->ndigits;
    int accum_weight = old_weight;
    int accum_ndigits = old_ndigits;

    // Enlarge buffers if new value has larger weight
    if (val->weight >= accum_weight) {
        accum_weight = val->weight + 1;  // +1 for carry space
        accum_ndigits = accum_ndigits + (accum_weight - old_weight);
    }
    // Or if carry space was used up
    else if (!accum->have_carry_space) {
        accum_weight++;
        accum_ndigits++;
    }

    // Expand right side if new value is wider
    int accum_rscale = accum_ndigits - accum_weight - 1;
    int val_rscale = val->ndigits - val->weight - 1;
    if (val_rscale > accum_rscale)
        accum_ndigits = accum_ndigits + (val_rscale - accum_rscale);

    // Reallocate buffers if size changed
    if (accum_ndigits != old_ndigits || accum_weight != old_weight) {
        int32 *new_pos_digits = palloc0(accum_ndigits * sizeof(int32));
        int32 *new_neg_digits = palloc0(accum_ndigits * sizeof(int32));
        int weightdiff = accum_weight - old_weight;

        // Copy existing data to new positions
        if (accum->pos_digits) {
            memcpy(&new_pos_digits[weightdiff], accum->pos_digits,
                   old_ndigits * sizeof(int32));
            pfree(accum->pos_digits);

            memcpy(&new_neg_digits[weightdiff], accum->neg_digits,
                   old_ndigits * sizeof(int32));
            pfree(accum->neg_digits);
        }

        accum->pos_digits = new_pos_digits;
        accum->neg_digits = new_neg_digits;
        accum->weight = accum_weight;
        accum->ndigits = accum_ndigits;
        accum->have_carry_space = true;
    }

    // Update scale to maximum
    if (val->dscale > accum->dscale)
        accum->dscale = val->dscale;
}
```