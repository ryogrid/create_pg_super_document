# gin_extract_tsvector

## Location
[src/backend/utils/adt/tsginidx.c:64-93](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsginidx.c#L64-L93)

## Overview
Extracts individual lexemes from a TSVector (text search vector) for GIN index construction, converting each lexeme into a separate indexable entry.

## Definition

```c
Datum
gin_extract_tsvector(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the extraction operator for GIN indexes on TSVector data types. It takes a TSVector containing multiple lexemes with their positions and weights, and decomposes it into an array of individual text entries that can be indexed separately by the GIN access method.

The function iterates through all lexemes stored in the TSVector, converting each lexeme from its internal string representation to a proper PostgreSQL text datum. This transformation is essential for GIN indexing, as it allows the index to treat each lexeme as an independent searchable key while preserving the original lexeme content.

The extracted entries become the keys that GIN uses to build its inverted index structure, enabling efficient full-text search operations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument (index 0):  - Input text search vector to extract from
  - Second argument (index 1):  - Output pointer to store number of extracted entries

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract TSVector from function arguments
  -  - Extract pointer argument for entry count
  -  - Get pointer to WordEntry array in TSVector
  -  - Get pointer to string data in TSVector
  -  - Allocate memory for entries array
  -  - Convert C string to PostgreSQL text type
  -  - Convert pointer to Datum type
  -  - Free TSVector if it was copied
  -  - Return entries array pointer
  -  - Text search vector data type
  -  - Individual lexeme entry structure
- Called from (representative examples):
  -  - Wrapper function for two-argument variant

## Notes and Other Information
- Returns NULL if input TSVector is empty (size = 0)
- Each lexeme is converted to a separate text datum for independent indexing
- Memory allocation uses palloc for PostgreSQL's memory context management
- Preserves exact lexeme content including length information
- Essential component of GIN operator class for TSVector indexing
- Used during index creation and maintenance operations
- Part of PostgreSQL's full-text search infrastructure

## Simplified Source

```c
Datum
gin_extract_tsvector(PG_FUNCTION_ARGS)
{
    TSVector vector = PG_GETARG_TSVECTOR(0);
    int32 *nentries = (int32 *) PG_GETARG_POINTER(1);
    Datum *entries = NULL;

    // Set number of entries to extract
    *nentries = vector->size;

    if (vector->size > 0) {
        WordEntry *we = ARRPTR(vector);  // Get word entries array
        entries = palloc(sizeof(Datum) * vector->size);

        // Extract each lexeme as a separate text entry
        for (int i = 0; i < vector->size; i++) {
            // Convert lexeme to text datum
            text *txt = cstring_to_text_with_len(STRPTR(vector) + we->pos, we->len);
            entries[i] = PointerGetDatum(txt);
            we++;
        }
    }

    PG_FREE_IF_COPY(vector, 0);
    PG_RETURN_POINTER(entries);
}
```