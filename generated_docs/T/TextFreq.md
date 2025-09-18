# TextFreq

## Location
src/backend/tsearch/ts_selfuncs.c: 39 - 45

## Overview
TextFreq is a lookup table structure used for binary searching through Most Common Elements (MCELEMs) in PostgreSQL's text search statistics, storing text elements with their frequency values.

## Definition
```c
typedef struct
{
    text       *element;
    float4      frequency;
} TextFreq;
```

## Detailed Description
TextFreq is a fundamental data structure in PostgreSQL's text search selectivity estimation system. It serves as a lookup table entry that pairs text elements with their statistical frequency values, enabling efficient binary search operations through arrays of Most Common Elements (MCELEMs). This structure is crucial for the query planner to estimate the selectivity of text search operations by providing frequency statistics for common lexemes in the database.

The structure is designed to work in conjunction with PostgreSQL's ANALYZE statistics collection, where text search columns are analyzed to determine the most frequent lexemes and their occurrence frequencies. These statistics are then used during query planning to estimate how many rows will match a given text search query.

## Parameters / Member Variables
- `element`: Pointer to a PostgreSQL text data type containing the lexeme or text element
- `frequency`: Float4 value representing the statistical frequency of occurrence for this element in the analyzed data

## Dependencies
- Functions called/Symbols referenced:
  - text (PostgreSQL text data type)
  - float4 (PostgreSQL float4 data type)
- Called from (representative examples):
  - mcelem_tsquery_selec
  - tsquery_opr_selec
  - compare_lexeme_textfreq

## Notes and Other Information
- Used specifically in src/backend/tsearch/ts_selfuncs.c for text search selectivity functions
- Works in conjunction with LexemeKey structure for binary search operations
- Part of PostgreSQL's statistics-based query optimization for full-text search
- The frequency values are generated during ANALYZE operations on tsvector columns
- Essential for accurate cost estimation in text search query planning