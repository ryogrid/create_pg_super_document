# hlfinditem

## Location
[src/backend/tsearch/ts_parse.c:464-498](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_parse.c#L464-L498)

## Overview
A static function that associates position data and matching query items with the most recently added word in the HeadlineParsedText structure during headline generation.

## Definition
```c
static void hlfinditem(HeadlineParsedText *prs, TSQuery query, int32 pos, char *buf, int buflen)
```

## Detailed Description
The `hlfinditem` function processes a lexeme to find matching query items and associates them with the last word added to the HeadlineParsedText structure. It iterates through all query items to find matches using string comparison. When multiple query items match the same lexeme, the function creates duplicate word entries (marked with repeated = 1) so each query item can be properly tracked. This is essential for accurate highlighting in full-text search results where a single word might match multiple search terms.

## Parameters / Member Variables
- `prs`: Pointer to HeadlineParsedText structure containing the parsed words and metadata
- `query`: TSQuery structure containing the search query with items to match against
- `pos`: Position of the lexeme in the original text (limited by LIMITPOS macro)
- `buf`: Character buffer containing the processed lexeme text
- `buflen`: Length of the lexeme buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - GETQUERY (macro to extract query items from TSQuery)
  - [repalloc](../r/repalloc.md) (for expanding words array when needed)
  - LIMITPOS (macro to limit position values)
  - [tsCompareString](../t/tsCompareString.md) (for comparing lexeme with query operands)
  - GETOPERAND (macro to get operand text from query)
  - memcpy (for duplicating word entries)
- Data structures used:
  - [HeadlineParsedText](../H/HeadlineParsedText.md)
  - TSQuery
  - QueryItem
  - [HeadlineWordEntry](../H/HeadlineWordEntry.md)
- Constants used:
  - QI_VAL (query item type for values)
- Called from (representative examples):
  - [addHLParsedLex](../a/addHLParsedLex.md)

## Notes and Other Information
- This is a static function, accessible only within ts_parse.c
- The function handles memory expansion automatically by doubling array size when needed
- When multiple query items match the same lexeme, duplicate word entries are created to maintain proper query-to-word relationships
- The 'repeated' flag is used to mark duplicate entries for the same lexeme
- Position values are constrained using the LIMITPOS macro to prevent overflow
- String comparison uses tsCompareString which handles prefix matching and other text search semantics
- This function is crucial for generating accurate highlights in search result snippets