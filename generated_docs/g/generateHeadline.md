# generateHeadline

## Location
[src/backend/tsearch/ts_parse.c:607-679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_parse.c#L607-L679)

## Overview
A function that generates the final text headline from a processed HeadlineParsedText structure, incorporating highlighting markup and fragment delimiters.

## Definition
```c
text *generateHeadline(HeadlineParsedText *prs)
```

## Detailed Description
The `generateHeadline` function is responsible for the final stage of headline generation in PostgreSQL's full-text search system. It converts the processed word entries in a HeadlineParsedText structure into a formatted text object with proper highlighting and fragment management. The function iterates through all word entries, handling fragment boundaries, selection markup (start/stop tags), word replacement, and memory management. It dynamically expands the output buffer as needed and manages fragment delimiters between different text fragments that contain matching terms.

## Parameters / Member Variables
- `prs`: Pointer to HeadlineParsedText structure containing processed words and formatting configuration

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (for initial memory allocation)
  - [repalloc](../r/repalloc.md) (for expanding output buffer)
  - memcpy (for copying text content and markup)
  - [pfree](../p/pfree.md) (for memory cleanup)
  - SET_VARSIZE (macro to set PostgreSQL text variable size)
- Data structures used:
  - [HeadlineParsedText](../H/HeadlineParsedText.md)
  - [HeadlineWordEntry](../H/HeadlineWordEntry.md)
  - [text](../t/text.md) (PostgreSQL text type)
- Constants/Macros used:
  - VARHDRSZ (variable header size for PostgreSQL text type)
- Called from (representative examples):
  - [ts_headline_byid_opt](../t/ts_headline_byid_opt.md)
  - [headline_json_value](../h/headline_json_value.md)

## Notes and Other Information
- Returns a PostgreSQL text object containing the formatted headline
- The function manages dynamic memory allocation, starting with 128 bytes and doubling as needed
- Fragment management tracks whether currently inside a fragment (infrag flag) and fragment count
- Fragment delimiters are inserted between multiple fragments containing matching terms
- Word processing handles several states:
  - Selected words: wrapped with start/stop selection markup
  - Replaced words: converted to single space characters
  - Skipped words: omitted from output entirely
  - Repeated words: skipped to avoid duplication
- Memory cleanup is performed for words that are not included in fragments
- The output buffer size calculation accounts for word length plus markup and delimiter lengths
- Fragment boundaries are determined by the 'in' flag of word entries
- The function properly handles PostgreSQL's variable-length text format with SET_VARSIZE
- Selected word highlighting uses configurable start/stop selection strings from the parsed text structure
- This function represents the final output generation stage of PostgreSQL's ts_headline functionality