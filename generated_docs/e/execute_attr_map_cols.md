# execute_attr_map_cols

## Location
[src/backend/access/common/tupconvert.c:252-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupconvert.c#L252-L298)

## Overview
Converts a bitmap of columns according to an attribute mapping, transforming column references from input schema to output schema while accommodating PostgreSQL's system column numbering.

## Definition

```c
Bitmapset *
execute_attr_map_cols(AttrMap *attrMap, Bitmapset *in_cols)
```
## Detailed Description
This function performs column bitmap conversion using an attribute map () to translate column references from one schema representation to another. The function is designed to handle PostgreSQL's column numbering system which includes both system columns (negative numbers) and user columns (positive numbers), with all bitmaps offset by .

The conversion process iterates through each possible output column position and determines the corresponding input column using the attribute map. System columns (negative attribute numbers) are mapped directly without transformation, while user columns are mapped according to the  array. The function builds a new bitmap containing only those output columns whose corresponding input columns are present in the input bitmap.

## Parameters / Member Variables
- : Pointer to an AttrMap structure containing the mapping from output column positions to input column numbers
- : Input bitmap representing a set of columns, offset by FirstLowInvalidHeapAttributeNumber

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_member](../b/bms_is_member.md) (checks if a column is present in the input bitmap)
  - [bms_add_member](../b/bms_add_member.md) (adds a column to the output bitmap)
  - [AttrMap](../A/AttrMap.md) (attribute mapping structure)
  - FirstLowInvalidHeapAttributeNumber (system constant for column numbering offset)
- Called from (representative examples):
  - [ExecGetInsertedCols](../E/ExecGetInsertedCols.md) (src/backend/executor/execUtils.c:1280)
  - [ExecGetUpdatedCols](../E/ExecGetUpdatedCols.md) (src/backend/executor/execUtils.c:1301)

## Notes and Other Information
- The function includes a fast path optimization for NULL input, returning NULL immediately
- System columns (attribute numbers < 0) are handled specially with direct mapping
- Attribute number 0 is skipped as it's invalid in PostgreSQL's column numbering
- The bitmap offset handling ensures compatibility with RangeTblEntry column bitmaps and other PostgreSQL structures that use this numbering convention
- Used primarily in executor utilities for determining which columns are involved in INSERT and UPDATE operations

## Simplified Source

```c
Bitmapset *
execute_attr_map_cols(AttrMap *attrMap, Bitmapset *in_cols) {
    Bitmapset *out_cols;
    int out_attnum;

    // Fast path for NULL input
    if (in_cols == NULL)
        return NULL;

    out_cols = NULL;

    // Iterate through all possible output columns
    for (out_attnum = FirstLowInvalidHeapAttributeNumber;
         out_attnum <= attrMap->maplen;
         out_attnum++) {
        int in_attnum;

        // Handle system columns (negative numbers)
        if (out_attnum < 0) {
            in_attnum = out_attnum; // Direct mapping for system columns
        }
        // Skip invalid attribute number 0
        else if (out_attnum == 0) {
            continue;
        }
        // Handle user columns
        else {
            in_attnum = attrMap->attnums[out_attnum - 1];
            if (in_attnum == 0)
                continue; // Skip unmapped columns
        }

        // If input column is present, add corresponding output column
        if (bms_is_member(in_attnum - FirstLowInvalidHeapAttributeNumber, in_cols))
            out_cols = bms_add_member(out_cols, out_attnum - FirstLowInvalidHeapAttributeNumber);
    }

    return out_cols;
}
```