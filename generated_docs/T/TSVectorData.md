# TSVectorData

## Location
src/include/tsearch/ts_type.h: 96 - 97

## Overview
TSVectorData is the complete data structure representing a PostgreSQL tsvector datum, containing the varlena header, size information, and arrays of word entries with their associated text data.

## Definition
```c
typedef struct
{
    int32       vl_len_;        /* varlena header (do not touch directly!) */
    int32       size;
    WordEntry   entries[FLEXIBLE_ARRAY_MEMBER];
    /* lexemes follow the entries[] array */
} TSVectorData;
```

## Detailed Description
TSVectorData represents the complete on-disk and in-memory format of a PostgreSQL tsvector. This structure serves as the foundation for PostgreSQL's full-text search functionality, storing both the metadata and actual text data for indexed documents.

The structure layout is carefully designed for efficient access:
1. A varlena header (vl_len_) for PostgreSQL's variable-length data management
2. A size field indicating the number of unique words/lexemes
3. A flexible array of WordEntry structures, one per unique word
4. Following the entries array, the actual lexeme (word) strings and their positional data

The data area after the entries array contains the lexeme strings in the order referenced by the WordEntry pos fields, optionally followed by WordEntryPosVector structures when positional information is present.

## Parameters / Member Variables
- `vl_len_`: Standard PostgreSQL varlena header containing the total size of the structure (managed by PostgreSQL's varlena system)
- `size`: Number of unique lexemes (words) stored in this tsvector
- `entries`: Flexible array of WordEntry structures, one for each unique lexeme, sorted by lexeme string for binary search

## Dependencies
- Functions called/Symbols referenced:
  - WordEntry (individual word entry metadata)
- Used by (representative examples):
  - TSVector (type alias for TSVectorData*)
  - DATAHDRSIZE (calculating header size)
  - TSVectorGetDatum (converting to PostgreSQL datum)

## Notes and Other Information
- This is the authoritative storage format for tsvector data in PostgreSQL
- The structure uses the PostgreSQL varlena (variable-length array) system for memory management
- WordEntry array is always kept sorted by lexeme string to enable binary search operations
- Lexeme data follows immediately after the entries array, referenced by WordEntry.pos offsets
- Position vectors (when present) are stored after their corresponding lexeme strings
- Total structure size is limited by the varlena system (approximately 1GB maximum)
- The DATAHDRSIZE macro provides the size of the fixed header portion
- CALCDATASIZE macro calculates total size given number of entries and string length