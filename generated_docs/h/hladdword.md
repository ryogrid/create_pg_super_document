# hladdword

## Location
[src/backend/tsearch/ts_parse.c:440-463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_parse.c#L440-L463)

## Overview
A static utility function that adds a word entry to the HeadlineParsedText structure's words array during text search headline generation.

## Definition

```c
static void
hladdword(HeadlineParsedText *prs, char *buf, int buflen, int type)
```
## Detailed Description
The  function is responsible for dynamically adding word entries to the  structure during the process of parsing text for headline generation. It manages memory allocation for the words array, automatically expanding it when needed by doubling its size. Each word entry contains metadata including the word type, length, and a copy of the actual word content. This function is part of PostgreSQL's text search framework for creating highlighted text snippets.

## Parameters / Member Variables
- `*prs`: Pointer to HeadlineParsedText structure containing the words array and tracking information
- `*buf`: Character buffer containing the word to be added
- `buflen`: Length of the word buffer in bytes
- `type`: Integer representing the type/category of the word being added
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

## Simplified Source

```c
static void hladdword(HeadlineParsedText *prs, char *buf, int buflen, int type) {
    // Expand words array if needed
    if (prs->curwords >= prs->lenwords) {
        prs->lenwords *= 2;
        prs->words = (HeadlineWordEntry *) repalloc(prs->words,
                                                  prs->lenwords * sizeof(HeadlineWordEntry));
    }

    // Initialize new word entry
    memset(&(prs->words[prs->curwords]), 0, sizeof(HeadlineWordEntry));
    prs->words[prs->curwords].type = (uint8) type;
    prs->words[prs->curwords].len = buflen;

    // Copy word content
    prs->words[prs->curwords].word = palloc(buflen);
    memcpy(prs->words[prs->curwords].word, buf, buflen);

    prs->curwords++;
}
```