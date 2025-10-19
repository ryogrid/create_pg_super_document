# rankSort

## Location
[src/bin/psql/crosstabview.c:588-635](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/crosstabview.c#L588-L635)

## Overview
Sorts pivot field columns based on their sort_value ranks and updates the rank field of each pivot_field to reflect the new sorted order.

## Definition

```c
static void
rankSort(int num_columns, pivot_field *piv_columns)
```
## Detailed Description
This function implements a custom sorting mechanism for pivot table columns in PostgreSQL's psql \crosstabview feature. It examines the sort_value field of each pivot_field to extract numeric ranking information, then sorts the columns according to these ranks. Valid rank values must be integers (positive, negative, or zero) matching the regular expression /^-?\d+$/. Invalid or missing rank values are treated as rank 0.

The function creates a temporary mapping array (hmap) that stores pairs of [rank_value, original_index] for each column, sorts this mapping using qsort with the rankCompare function, then updates the rank field of each pivot_field to reflect the new sorted position.

## Parameters / Member Variables
- `num_columns`: Number of pivot field columns to be sorted
- `piv_columns`: Array of pivot_field structures containing the columns to sort

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md) (memory allocation for temporary mapping array)
  - qsort (standard library sorting function)
  - [rankCompare](rankCompare.md) (custom comparison function for sorting rank pairs)
  - [pg_free](../p/pg_free.md) (memory deallocation)
- Called from (representative examples):
  - [PrintResultInCrosstab](../P/PrintResultInCrosstab.md) (main crosstab processing function)

## Notes and Other Information
- Only processes sort_value fields that contain valid integer strings (including negative numbers)
- Invalid rank values are silently treated as rank 0 rather than causing errors
- Uses a two-phase approach: first extract and sort ranks, then update the original array
- The rank field gets updated to reflect the final sorted position (0-based indexing)
- Memory allocation is performed for the temporary mapping array and properly freed afterward
- This enables custom column ordering in crosstab output based on user-specified ranking criteria

## Simplified Source

```c
static void rankSort(int num_columns, pivot_field *piv_columns) {
    // Create mapping array: [rank_value, original_index, ...]
    int *rank_map = pg_malloc(sizeof(int) * num_columns * 2);

    // Extract rank values from sort_value fields
    for (int i = 0; i < num_columns; i++) {
        char *val = piv_columns[i].sort_value;

        // Check if sort_value is a valid integer (positive or negative)
        if (val &&
            ((*val == '-' && strspn(val + 1, "0123456789") == strlen(val + 1)) ||
             strspn(val, "0123456789") == strlen(val))) {
            rank_map[i * 2] = atoi(val);      // rank value
            rank_map[i * 2 + 1] = i;          // original index
        } else {
            // Invalid rank treated as 0
            rank_map[i * 2] = 0;
            rank_map[i * 2 + 1] = i;
        }
    }

    // Sort the mapping by rank values
    qsort(rank_map, num_columns, sizeof(int) * 2, rankCompare);

    // Update the rank field based on sorted order
    for (int i = 0; i < num_columns; i++) {
        int original_index = rank_map[i * 2 + 1];
        piv_columns[original_index].rank = i;
    }

    pg_free(rank_map);
}
```