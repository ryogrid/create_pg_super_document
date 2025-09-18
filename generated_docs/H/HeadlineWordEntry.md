# HeadlineWordEntry

## Location
[src/include/tsearch/ts_public.h:71-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_public.h#L71-L88)

## Overview
HeadlineWordEntry is a structure that represents a single word token in text search headline generation, containing metadata about whether the token should be highlighted, its position, and references to matching query operands.

## Definition


## Detailed Description
HeadlineWordEntry is a core data structure used in PostgreSQL's text search system for generating headlines (text snippets with highlighted search terms). Each structure represents a single token from parsed text and contains both the token's content and various flags that control how it should be processed during headline generation.

The structure uses bit fields in a uint32 to efficiently store multiple boolean flags, along with the token type and length. This compact representation is important for performance when processing large amounts of text. When a token matches multiple query operands, the system creates duplicate entries marked with the 'repeated' flag to hold all matching item pointers.

## Parameters / Member Variables
- : Boolean flag indicating whether this token should be highlighted in the output
- : Boolean flag indicating whether this token is part of the final headline
- : Boolean flag indicating whether this token should be replaced with a space in output
- : Boolean flag marking duplicate entries created to hold multiple item pointers
- : Boolean flag indicating whether this token should be skipped (not included in output)
- : 3 bits reserved for future use
- : 8-bit field containing the parser's token category classification
- : 16-bit field containing the length of the token text
- : WordEntryPos structure containing the position information of the token
- : Pointer to the token text (note: not null-terminated)
- : Pointer to a matching QueryOperand from the search query, or NULL if no match

## Dependencies
- Functions called/Symbols referenced:
  - WordEntryPos
  - QueryOperand
- Called from (representative examples):
  - [hladdword](../h/hladdword.md) (src/backend/tsearch/ts_parse.c:445)
  - [hlfinditem](../h/hlfinditem.md) (src/backend/tsearch/ts_parse.c:468)
  - [generateHeadline](../g/generateHeadline.md) (src/backend/tsearch/ts_parse.c:615)
  - [ts_headline_byid_opt](../t/ts_headline_byid_opt.md) (src/backend/tsearch/wparser.c:310)
  - [CoverPos](../C/CoverPos.md) (src/backend/tsearch/wparser_def.c:1968)

## Notes and Other Information
- The word field is not null-terminated, so the len field must be used to determine the actual token length
- When a token matches multiple query operands, duplicate HeadlineWordEntry structures are created with repeated=1
- Only the first entry (repeated=0) should be used for token processing; duplicates are only for holding additional item pointers
- This structure is part of the HeadlineParsedText array used in headline generation algorithms
- The bit field layout optimizes memory usage while maintaining fast access to frequently-checked flags