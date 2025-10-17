# tsvector_delete_by_indices

## Location
[src/backend/utils/adt/tsvector_op.c:464-553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L464-L553)

## Overview
An internal static function that creates a new TSVector by removing lexemes at specified indices from an existing TSVector.

## Definition

```c
static TSVector
tsvector_delete_by_indices(TSVector tsv, int *indices_to_delete,
						   int indices_count)
```
## Detailed Description
The  function performs selective deletion of lexemes from a TSVector based on an array of indices. It creates a new TSVector containing all lexemes except those at the specified positions, preserving the alphabetical ordering and maintaining position/weight information for retained lexemes.

The function operates in several phases:
1. **Index Preprocessing**: Sorts the indices_to_delete array and removes duplicates using qsort and qunique
2. **Memory Allocation**: Allocates memory for the output TSVector (initially overestimating size)
3. **Selective Copying**: Iterates through the source TSVector, copying only lexemes not marked for deletion
4. **Data Preservation**: For each retained lexeme, copies both the lexeme text and any associated position/weight data
5. **Memory Alignment**: Ensures proper alignment of position data using SHORTALIGN
6. **Size Correction**: Sets the final size of the output TSVector based on actual data copied

## Parameters / Member Variables
- `tsv`: Source TSVector from which to delete lexemes
- `*indices_to_delete`: Array of lexeme indices to remove (gets modified by sorting/deduplication)
- `indices_count`: Number of elements in the indices array
## Dependencies
- Functions called/Symbols referenced:
  - qsort (sorts indices array using compare_int)
  - [qunique](../q/qunique.md) (removes duplicate indices using compare_int)  
  - [compare_int](../c/compare_int.md) (comparator function for integer sorting)
  - [palloc0](../p/palloc0.md) (PostgreSQL memory allocation)
  - memcpy (memory copy operations)
  - ARRPTR/STRPTR (TSVector access macros)
  - POSDATALEN (position data length calculation)
  - SHORTALIGN (memory alignment for position data)
  - SET_VARSIZE/CALCDATASIZE (TSVector size management)
- Called from:
  - [tsvector_delete_str](tsvector_delete_str.md) (for single lexeme deletion)
  - [tsvector_delete_arr](tsvector_delete_arr.md) (for multiple lexeme deletion by text array)

## Notes and Other Information
- The indices_to_delete array is modified during execution (sorted and deduplicated)
- Includes bounds checking via Assert to ensure all specified indices are valid
- Preserves position and weight information for all retained lexemes
- Uses memory alignment requirements for position data storage
- Returns a completely new TSVector rather than modifying the input
- Critical for implementing various TSVector deletion operations in PostgreSQL's text search functionality
- Efficiently handles both single and multiple lexeme deletions with O(n) complexity after initial sorting

## Simplified Source

```c
static TSVector tsvector_delete_by_indices(TSVector tsv, int *indices_to_delete, int indices_count) {
    WordEntry *input_entries = ARRPTR(tsv);
    char *input_data = STRPTR(tsv);

    // Sort and deduplicate indices for efficient processing
    if (indices_count > 1) {
        qsort(indices_to_delete, indices_count, sizeof(int), compare_int);
        indices_count = qunique(indices_to_delete, indices_count, sizeof(int), compare_int);
    }

    // Allocate output TSVector (initially overestimate size)
    TSVector output = (TSVector) palloc0(VARSIZE(tsv));
    output->size = tsv->size - indices_count;

    // Prepare for copying data
    WordEntry *output_entries = ARRPTR(output);
    char *output_data = STRPTR(output);
    int data_offset = 0;
    int output_index = 0;
    int delete_index = 0;

    // Copy lexemes, skipping those marked for deletion
    for (int i = 0; i < tsv->size; i++) {
        // Check if current lexeme should be deleted
        if (delete_index < indices_count && i == indices_to_delete[delete_index]) {
            delete_index++;
            continue; // Skip this lexeme
        }

        // Copy lexeme text
        memcpy(output_data + data_offset, input_data + input_entries[i].pos, input_entries[i].len);

        // Set up output entry
        output_entries[output_index].haspos = input_entries[i].haspos;
        output_entries[output_index].len = input_entries[i].len;
        output_entries[output_index].pos = data_offset;
        data_offset += input_entries[i].len;

        // Copy position data if present
        if (input_entries[i].haspos) {
            int pos_data_len = POSDATALEN(tsv, &input_entries[i]) * sizeof(WordEntryPos) + sizeof(uint16);

            // Align data for position storage
            data_offset = SHORTALIGN(data_offset);
            memcpy(output_data + data_offset,
                   STRPTR(tsv) + SHORTALIGN(input_entries[i].pos + input_entries[i].len),
                   pos_data_len);
            data_offset += pos_data_len;
        }

        output_index++;
    }

    // Verify all indices were processed
    Assert(delete_index == indices_count);

    // Set final size based on actual data copied
    SET_VARSIZE(output, CALCDATASIZE(output->size, data_offset));
    return output;
}
```