# LexemeKey

## Location
src/backend/tsearch/ts_selfuncs.c: 46 - 55

## Overview
LexemeKey is a key structure used for binary searching through arrays of TextFreq elements, providing efficient lookup capabilities in PostgreSQL's text search selectivity estimation system.

## Definition
```c
typedef struct
{
    char       *lexeme;
    int         length;
} LexemeKey;
```

## Detailed Description
LexemeKey serves as a search key structure specifically designed for binary search operations through arrays of TextFreq structures. It represents a lexeme (word token) with its associated length, enabling efficient comparison and lookup operations during text search selectivity estimation. This structure is crucial for the query planner's ability to quickly locate frequency statistics for specific lexemes when estimating the selectivity of text search queries.

The structure works as a companion to TextFreq, where LexemeKey provides the search criteria while TextFreq contains the actual statistical data. During query planning, the system uses LexemeKey to perform binary searches through sorted arrays of TextFreq elements to find frequency information for specific lexemes present in tsquery expressions.

## Parameters / Member Variables
- `lexeme`: Pointer to a character string containing the lexeme text to search for
- `length`: Integer value representing the length of the lexeme string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - char (C character data type)
  - int (C integer data type)
- Called from (representative examples):
  - tsquery_opr_selec
  - compare_lexeme_textfreq

## Notes and Other Information
- Used specifically in src/backend/tsearch/ts_selfuncs.c for text search selectivity functions
- Works in conjunction with TextFreq structure for binary search operations
- The compare_lexeme_textfreq function uses LexemeKey as the search key to locate matching TextFreq entries
- Essential for efficient lookup of lexeme frequency statistics during query planning
- The length field helps optimize string comparisons by allowing length-based short-circuiting
- Part of PostgreSQL's statistics-based optimization system for full-text search queries