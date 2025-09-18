# add_to_tsvector

## Location
[src/backend/tsearch/to_tsany.c:443-491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L443-L491)

## Overview
A static callback function that parses lexemes from a JSON element value and adds them to a TSVectorBuildState for building text search vectors.

## Definition
```c
static void add_to_tsvector(void *_state, char *elem_value, int elem_len)
```

## Detailed Description
This function serves as a callback for JSON processing that takes individual JSON element values and converts them into parsed words for full-text search indexing. It initializes the ParsedText structure on first use, then delegates to `parsetext` to extract and process lexemes from the element value. The function maintains position tracking to create artificial breaks between JSON elements, ensuring that phrase searches don't incorrectly match words from adjacent elements as if they were adjacent in the original text.

## Parameters / Member Variables
- `_state` (void *): Pointer to TSVectorBuildState cast to void*, contains parsing configuration and accumulated results
- `elem_value` (char *): String content of the JSON element to be parsed
- `elem_len` (int): Length of the element value string

## Dependencies
- Functions called/Symbols referenced:
  - [TSVectorBuildState](../T/TSVectorBuildState.md)
  - ParsedText
  - ParsedWord
  - [palloc](../p/palloc.md)
  - [parsetext](../p/parsetext.md)
- Called from (representative examples):
  - [jsonb_to_tsvector_worker](../j/jsonb_to_tsvector_worker.md)
  - [json_to_tsvector_worker](../j/json_to_tsvector_worker.md)

## Notes and Other Information
- Static function used internally within the JSON to TSVector conversion process
- Lazy initialization of the words array with an initial size of 16 ParsedWord elements
- Position tracking (`prs->pos`) is incremented between elements to prevent phrase search artifacts
- The function maintains proper separation between different JSON elements for accurate phrase search behavior
- Memory management relies on palloc for ParsedWord array allocation, with automatic reallocation handled by parsetext when needed