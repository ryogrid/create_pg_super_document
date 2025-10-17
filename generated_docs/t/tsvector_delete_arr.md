# tsvector_delete_arr

## Location
[src/backend/utils/adt/tsvector_op.c:578-631](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L578-L631)

## Overview
Deletes multiple lexemes from a TSVector by accepting an array of lexemes to remove, implementing the user-level ts_delete(tsvector, text[]) function.

## Definition
```c
Datum tsvector_delete_arr(PG_FUNCTION_ARGS)
```

## Detailed Description
This function removes multiple lexemes from a TSVector by processing an array of text elements. It deconstructs the input array into individual lexeme strings and performs binary search for each lexeme in the TSVector. The function is optimized for the typical use case where the array of lexemes to delete is relatively small compared to the TSVector size.

For each non-null lexeme in the array, the function performs a binary search to find its position in the TSVector. Found positions are collected in a skip_indices array, which is then passed to tsvector_delete_by_indices for efficient batch deletion. The function handles null array elements gracefully by ignoring them.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Input TSVector from which to delete lexemes
- `PG_FUNCTION_ARGS[1]`: Array of text elements containing lexemes to delete

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR - Extract TSVector argument
  - PG_GETARG_ARRAYTYPE_P - Extract array argument
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md) - Decompose array into elements
  - VARDATA - Get pointer to variable-length data
  - VARSIZE - Get size of variable-length data
  - [tsvector_bsearch](tsvector_bsearch.md) - Binary search for lexeme in TSVector
  - [tsvector_delete_by_indices](tsvector_delete_by_indices.md) - Helper to delete lexemes by index array
  - [palloc0](../p/palloc0.md) - Allocate zero-initialized memory
  - [pfree](../p/pfree.md) - Free allocated memory
  - PG_FREE_IF_COPY - Free copied arguments if needed
  - PG_RETURN_POINTER - Return result pointer
- Called from (representative examples):
  - No direct references found (likely called through SQL function dispatch)

## Notes and Other Information
- Optimized for small arrays of lexemes to delete relative to TSVector size
- Handles null array elements by skipping them during processing
- Uses binary search for efficient lexeme lookup in the sorted TSVector structure
- Collects all matching indices before performing batch deletion for efficiency
- Proper memory management with cleanup of temporary arrays and copied arguments
- Part of PostgreSQL's full-text search functionality for batch TSVector manipulation
- More efficient than calling tsvector_delete_str multiple times for multiple deletions

## Simplified Source

```c
Datum tsvector_delete_arr(PG_FUNCTION_ARGS) {
    TSVector input_tsvector = PG_GETARG_TSVECTOR(0);
    ArrayType *lexeme_array = PG_GETARG_ARRAYTYPE_P(1);

    // Deconstruct the array into individual elements
    Datum *lexemes;
    bool *nulls;
    int num_lexemes;
    deconstruct_array_builtin(lexeme_array, TEXTOID, &lexemes, &nulls, &num_lexemes);

    // Allocate array to collect indices of lexemes to delete
    int *indices_to_delete = palloc0(num_lexemes * sizeof(int));
    int delete_count = 0;

    // Find indices of lexemes that exist in the TSVector
    for (int i = 0; i < num_lexemes; i++) {
        if (nulls[i]) continue; // Skip NULL elements

        // Extract lexeme text and length
        char *lexeme_text = VARDATA(lexemes[i]);
        int lexeme_len = VARSIZE(lexemes[i]) - VARHDRSZ;

        // Search for lexeme in TSVector
        int lexeme_pos = tsvector_bsearch(input_tsvector, lexeme_text, lexeme_len);

        // Add to deletion list if found
        if (lexeme_pos >= 0) {
            indices_to_delete[delete_count++] = lexeme_pos;
        }
    }

    // Perform batch deletion using collected indices
    TSVector output_tsvector = tsvector_delete_by_indices(input_tsvector, indices_to_delete, delete_count);

    // Clean up memory
    pfree(indices_to_delete);
    PG_FREE_IF_COPY(input_tsvector, 0);
    PG_FREE_IF_COPY(lexeme_array, 1);

    PG_RETURN_POINTER(output_tsvector);
}
```