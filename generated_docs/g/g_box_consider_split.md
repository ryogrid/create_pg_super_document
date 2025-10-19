# g_box_consider_split

## Location
[src/backend/access/gist/gistproc.c:351-459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L351-L459)

## Overview
A static inline function that evaluates a potential split candidate for GiST box splitting and selects it if it provides a better split than the currently selected one.

## Definition
```c
static inline void g_box_consider_split(ConsiderSplitContext *context, int dimNum, float8 rightLower, int minLeftCount, float8 leftUpper, int maxLeftCount)
```

## Detailed Description
The `g_box_consider_split` function is a core component of the GiST box splitting algorithm that evaluates whether a proposed split is better than the currently selected split. It implements a sophisticated split selection algorithm that considers multiple criteria:

1. **Distribution Ratio**: Calculates how evenly entries would be distributed between left and right groups
2. **Overlap Analysis**: Measures the overlap between the resulting bounding boxes
3. **Range Consideration**: Evaluates the range of the bounding box in the split dimension

The function uses different comparison criteria depending on whether the candidate split is in the same dimension as the current best split or in a different dimension. For cross-dimensional comparisons, it uses non-negative overlap and range as criteria to avoid creating overly prolonged MBRs (Minimum Bounding Rectangles) that would hurt search performance.

## Parameters / Member Variables
- `context`: Pointer to ConsiderSplitContext structure containing current split state and bounding box information
- `dimNum`: Dimension number (0 for x-axis, 1 for y-axis) being considered for the split
- `rightLower`: Lower bound coordinate of the right group in the split dimension
- `minLeftCount`: Minimum number of entries that would be in the left group
- `leftUpper`: Upper bound coordinate of the left group in the split dimension
- `maxLeftCount`: Maximum number of entries that would be in the left group

## Dependencies
- Functions called/Symbols referenced:
  - [ConsiderSplitContext](../C/ConsiderSplitContext.md) (structure type)
  - [float4_div](../f/float4_div.md) (floating-point division)
  - [float8_mi](../f/float8_mi.md) (floating-point subtraction)
  - [float8_div](../f/float8_div.md) (double-precision floating-point division)
  - [non_negative](../n/non_negative.md) (utility function to ensure non-negative values)
  - LIMIT_RATIO (constant defining acceptable split ratio threshold)
- Called from (representative examples):
  - [gist_box_picksplit](gist_box_picksplit.md) (called at lines 636 and 672)

## Notes and Other Information
- The function implements an enhanced version of the R-tree splitting algorithm optimized for PostgreSQL's GiST implementation
- Uses the LIMIT_RATIO threshold to ensure splits maintain reasonable balance between left and right groups
- The cross-dimensional comparison logic (using non-negative overlap) is specifically designed to prevent the creation of highly elongated MBRs that would degrade search performance
- The algorithm prioritizes splits with minimal overlap within the same dimension, but uses range as a secondary criterion across dimensions to maintain more quadratic (balanced) MBR shapes
- This function is critical for maintaining the performance characteristics of GiST indexes on geometric data types like boxes, polygons, and circles

## Simplified Source

```c
static inline void g_box_consider_split(ConsiderSplitContext *context, int dimNum,
                                      float8 rightLower, int minLeftCount,
                                      float8 leftUpper, int maxLeftCount)
{
    int leftCount, rightCount;
    float4 ratio, overlap;
    float8 range;

    // Calculate balanced distribution of entries
    if (minLeftCount >= (context->entriesCount + 1) / 2) {
        leftCount = minLeftCount;
    } else if (maxLeftCount <= context->entriesCount / 2) {
        leftCount = maxLeftCount;
    } else {
        leftCount = context->entriesCount / 2;
    }
    rightCount = context->entriesCount - leftCount;

    // Calculate split ratio (size of smaller group / total entries)
    ratio = float4_div(Min(leftCount, rightCount), context->entriesCount);

    // Only consider splits with acceptable balance
    if (ratio > LIMIT_RATIO)
    {
        bool selectthis = false;

        // Calculate overlap and range for this dimension
        if (dimNum == 0)
            range = float8_mi(context->boundingBox.high.x, context->boundingBox.low.x);
        else
            range = float8_mi(context->boundingBox.high.y, context->boundingBox.low.y);

        overlap = float8_div(float8_mi(leftUpper, rightLower), range);

        // Select this split if it's the first one
        if (context->first) {
            selectthis = true;
        }
        // Within same dimension: prefer smaller overlap, then better ratio
        else if (context->dim == dimNum) {
            if (overlap < context->overlap ||
                (overlap == context->overlap && ratio > context->ratio))
                selectthis = true;
        }
        // Across dimensions: prefer smaller non-negative overlap, then larger range
        else {
            if (non_negative(overlap) < non_negative(context->overlap) ||
                (range > context->range &&
                 non_negative(overlap) <= non_negative(context->overlap)))
                selectthis = true;
        }

        // Update context with selected split
        if (selectthis) {
            context->first = false;
            context->ratio = ratio;
            context->range = range;
            context->overlap = overlap;
            context->rightLower = rightLower;
            context->leftUpper = leftUpper;
            context->dim = dimNum;
        }
    }
}
```