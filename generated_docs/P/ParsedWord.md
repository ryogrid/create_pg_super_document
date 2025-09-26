# ParsedWord

## Location
[src/include/tsearch/ts_utils.h:100-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_utils.h#L100-L107)

## Overview
ParsedWord is a structure that represents a single lexeme (word) that has been parsed and processed by PostgreSQL's text search functionality. It stores the lexeme along with positional information and metadata flags used in full-text search operations.

## Definition

```c
typedef struct
{
	ParsedWord *words;
	int32		lenwords;
	int32		curwords;
	int32		pos;
} ParsedText;
```
## Detailed Description
ParsedWord is a fundamental data structure in PostgreSQL's text search system, defined in . This structure represents a single parsed word (lexeme) extracted from text during the text search parsing process. It contains both the actual word text and associated metadata including positional information and processing flags.

The structure supports two modes for storing positional information: a single position stored directly in the  field of the union, or multiple positions stored in an array pointed to by . The choice between these modes depends on whether the word appears multiple times in the source text.

The structure is primarily used during the text parsing phase of full-text search operations, where raw text is tokenized, normalized, and converted into a format suitable for indexing and searching.

## Parameters / Member Variables
- : Bit flags for special processing options. Currently only supports  (0x02) to indicate prefix matching
- : The length of the word string pointed to by the  field
- : Number of lexeme variants for this word (used in morphological processing)
- : Allocated size of the  array when multiple positions are stored
- : Union containing either a single position () or pointer to position array ()
  - : Single position value when word appears once
  - : Pointer to array of positions when word appears multiple times. Array format: apos[0] contains count, subsequent elements contain positions. Limited to MAXNUMPOS (256) elements
- : Pointer to the null-terminated word string

## Dependencies
- Functions called/Symbols referenced:
  - TSL_PREFIX (flag constant from ts_public.h)
  - MAXNUMPOS (constant from ts_type.h, value 256)
  - Standard C types (uint16, char*)

- Called from (representative examples):
  - [compareWORD](../c/compareWORD.md) (src/backend/tsearch/to_tsany.c:61-70)
  - [uniqueWORD](../u/uniqueWORD.md) (src/backend/tsearch/to_tsany.c:77-99)
  - [to_tsvector_byid](../t/to_tsvector_byid.md) (src/backend/tsearch/to_tsany.c:254-258)
  - [add_to_tsvector](../a/add_to_tsvector.md) (src/backend/tsearch/to_tsany.c:456)
  - [pushval_morph](../p/pushval_morph.md) (src/backend/tsearch/to_tsany.c:506)
  - [parsetext](../p/parsetext.md) (src/backend/tsearch/ts_parse.c:413)
  - [tsvector_update_trigger](../t/tsvector_update_trigger.md) (src/backend/utils/adt/tsvector_op.c:2842)
  - ParsedText structure (contains array of ParsedWord elements)

## Notes and Other Information
- The structure uses a union for position storage to optimize memory usage: single positions use less memory than arrays
- The  array uses a special format where the first element (apos[0]) stores the count of actual position values
- Position arrays are limited to MAXNUMPOS (256) elements to prevent excessive memory usage
- This structure is part of the text search parsing pipeline that converts raw text into searchable tsvector format
- The  field supports PostgreSQL's ability to handle multiple morphological forms of the same word
- Memory management for the  pointer and  array is handled by the calling functions in the text search subsystem