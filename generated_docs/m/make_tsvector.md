# make_tsvector

## Location
[src/backend/tsearch/to_tsany.c:165-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L165-L242)

## Overview
Constructs a TSVector data structure from parsed text, creating the final binary representation used for PostgreSQL's text search functionality.

## Definition

```c
TSVector
make_tsvector(ParsedText *prs)
```
## Detailed Description
 is responsible for creating the final TSVector data structure from a ParsedText input. This function performs several critical operations:

1. **Deduplication**: Calls  to merge duplicate words and consolidate their position information into arrays.

2. **Space calculation**: Computes the total space needed for the TSVector, including:
   - Space for WordEntry headers
   - Space for word strings
   - Space for position arrays (properly aligned)

3. **Memory allocation**: Allocates and initializes the TSVector structure using PostgreSQL's memory management.

4. **Data serialization**: Populates the TSVector with:
   - Word entries containing length, position offset, and haspos flags
   - Word strings stored in a contiguous buffer
   - Position arrays for each word (when present) with proper alignment

5. **Cleanup**: Frees all temporary memory allocated during parsing.

The function enforces size limits () and handles proper alignment requirements for position data. Each word's positions are stored with weight information (defaulting to weight 0) and position values.

## Parameters / Member Variables
- `*prs`: Pointer to ParsedText structure containing the parsed words and their positions
## Dependencies
- Functions called/Symbols referenced:
  - : Deduplicates words and consolidates positions
  - : Alignment macro for position data
  - : Calculates total size needed for TSVector
  - : PostgreSQL zero-initialized memory allocation
  - : PostgreSQL memory deallocation
  - : Sets the variable-length header size
  - : Gets pointer to WordEntry array
  - : Gets pointer to string data area
  - : Gets pointer to position data for a word
  - : Sets weight in WordEntryPos
  - : Sets position in WordEntryPos
  - : Standard C memory copy function
- Called from (representative examples):
  - : Main entry point for text-to-tsvector conversion
  - : JSON/JSONB to tsvector conversion
  - : JSON to tsvector conversion
  - : Trigger function for automatic tsvector updates

## Notes and Other Information
- This function represents the final stage of text-to-tsvector conversion
- The resulting TSVector follows PostgreSQL's internal binary format for efficient storage and searching
- Position data is aligned on 2-byte boundaries for performance
- Maximum string length is enforced to prevent excessive memory usage
- All intermediate parsing data is freed, making this function responsible for cleanup
- Located at lines 165-242 in 
- The function modifies and frees the input ParsedText structure as part of its operation

## Simplified Source

```c
TSVector make_tsvector(ParsedText *prs) {
    int i, j, string_length = 0, total_length;
    TSVector result;
    WordEntry *word_entries;
    char *string_data;
    int string_offset;

    // Remove duplicates and consolidate positions
    if (prs->curwords > 0)
        prs->curwords = uniqueWORD(prs->words, prs->curwords);

    // Calculate space needed for words and position arrays
    for (i = 0; i < prs->curwords; i++) {
        string_length += prs->words[i].len;
        if (prs->words[i].alen) {
            string_length = SHORTALIGN(string_length);
            string_length += sizeof(uint16) + prs->words[i].pos.apos[0] * sizeof(WordEntryPos);
        }
    }

    // Check size limits
    if (string_length > MAXSTRPOS)
        ereport(ERROR, "string is too long for tsvector");

    // Allocate TSVector structure
    total_length = CALCDATASIZE(prs->curwords, string_length);
    result = (TSVector) palloc0(total_length);
    SET_VARSIZE(result, total_length);
    result->size = prs->curwords;

    // Setup pointers for data areas
    word_entries = ARRPTR(result);
    string_data = STRPTR(result);
    string_offset = 0;

    // Populate TSVector with words and positions
    for (i = 0; i < prs->curwords; i++) {
        // Store word entry metadata
        word_entries->len = prs->words[i].len;
        word_entries->pos = string_offset;

        // Copy word string
        memcpy(string_data + string_offset, prs->words[i].word, prs->words[i].len);
        string_offset += prs->words[i].len;
        pfree(prs->words[i].word);

        // Handle position data if present
        if (prs->words[i].alen) {
            int position_count = prs->words[i].pos.apos[0];
            WordEntryPos *position_ptr;

            word_entries->haspos = 1;
            string_offset = SHORTALIGN(string_offset);
            *(uint16 *) (string_data + string_offset) = (uint16) position_count;

            position_ptr = POSDATAPTR(result, word_entries);
            for (j = 0; j < position_count; j++) {
                WEP_SETWEIGHT(position_ptr[j], 0);  // default weight
                WEP_SETPOS(position_ptr[j], prs->words[i].pos.apos[j + 1]);
            }
            string_offset += sizeof(uint16) + position_count * sizeof(WordEntryPos);
            pfree(prs->words[i].pos.apos);
        } else {
            word_entries->haspos = 0;
        }
        word_entries++;
    }

    // Cleanup parsed text structure
    if (prs->words)
        pfree(prs->words);

    return result;
}
```