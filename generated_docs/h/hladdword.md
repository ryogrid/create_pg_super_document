# hladdword

## Location
src/backend/tsearch/ts_parse.c: 440 - 463

## Overview
A static utility function that adds a word entry to the HeadlineParsedText structure's words array during text search headline generation.

## Definition


## Detailed Description
The  function is responsible for dynamically adding word entries to the  structure during the process of parsing text for headline generation. It manages memory allocation for the words array, automatically expanding it when needed by doubling its size. Each word entry contains metadata including the word type, length, and a copy of the actual word content. This function is part of PostgreSQL's text search framework for creating highlighted text snippets.

## Parameters / Member Variables
- : Pointer to HeadlineParsedText structure containing the words array and tracking information
- : Character buffer containing the word to be added
- : Length of the word buffer in bytes
- : Integer representing the type/category of the word being added

## Dependencies
- Functions called/Symbols referenced:
  - [repalloc](../r/repalloc.md) (for expanding the words array when needed)
  - [palloc](../p/palloc.md) (for allocating memory for individual word content)
  - memset (for initializing word entry structure)
  - memcpy (for copying word content)
- Data structures used:
  - [HeadlineParsedText](../H/HeadlineParsedText.md)
  - [HeadlineWordEntry](../H/HeadlineWordEntry.md)
- Called from (representative examples):
  - [addHLParsedLex](../a/addHLParsedLex.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the ts_parse.c file
- The function automatically manages memory expansion by doubling the words array size when capacity is reached
- Each word entry is zero-initialized before setting its properties to ensure clean state
- Memory allocation is handled through PostgreSQL's memory management functions (palloc/repalloc)
- The function is part of the headline generation framework used in full-text search operations