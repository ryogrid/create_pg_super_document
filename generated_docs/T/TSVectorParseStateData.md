# TSVectorParseStateData

## Location
src/backend/utils/adt/tsvector_parser.c: 37 - 56

## Overview
TSVectorParseStateData is a private state structure used by PostgreSQL's tsvector parser to maintain parsing context and configuration during text search vector parsing operations.

## Definition

```c
struct TSVectorParseStateData
{
	char	   *prsbuf;			/* next input character */
	char	   *bufstart;		/* whole string (used only for errors) */
	char	   *word;			/* buffer to hold the current word */
	int			len;			/* size in bytes allocated for 'word' */
	int			eml;			/* max bytes per character */
	bool		oprisdelim;		/* treat ! | * ( ) as delimiters? */
	bool		is_tsquery;		/* say "tsquery" not "tsvector" in errors? */
	bool		is_web;			/* we're in websearch_to_tsquery() */
	Node	   *escontext;		/* for soft error reporting */
};
```
## Detailed Description
TSVectorParseStateData serves as the core parsing state structure for PostgreSQL's text search functionality. It maintains all necessary information to parse both tsvector and tsquery inputs, providing a unified parsing framework with configurable behavior through boolean flags.

The structure is designed to be opaque to external code (defined in tsvector_parser.c but declared as an incomplete type in ts_utils.h), ensuring encapsulation of the parsing implementation details. It supports different parsing modes for various text search contexts, including regular tsvector parsing, tsquery parsing, and web search query parsing.

The parser state tracks the current position in the input string, manages a dynamically allocated word buffer, and maintains parsing configuration through boolean flags that control delimiter treatment and error message formatting.

## Parameters / Member Variables
- : Pointer to the current position in the input string being parsed
- : Pointer to the beginning of the entire input string, used for error reporting and context
- : Dynamically allocated buffer to hold the current word being parsed
- : Size in bytes currently allocated for the word buffer (initially 32 bytes)
- : Maximum bytes per character for the current database encoding
- : Boolean flag indicating whether operators (\! | * ( )) should be treated as delimiters
- : Boolean flag affecting error message content ("tsquery" vs "tsvector" in messages)
- : Boolean flag indicating parsing is for websearch_to_tsquery() functionality
- : Error context node for soft error reporting, allowing errors to be captured rather than thrown

## Dependencies
- Functions called/Symbols referenced:
  - word (member variable)
  - TSVectorParseState (typedef pointer to this struct)
- Called from (representative examples):
  - init_tsvector_parser (initializes and allocates this structure)
  - Various tsvector/tsquery parsing functions via TSVectorParseState pointer

## Notes and Other Information
- The structure is deliberately opaque, with the complete definition hidden in the implementation file
- Both oprisdelim and is_tsquery flags are typically set together in current usage but kept separate for clarity
- The word buffer is dynamically resized as needed during parsing
- The eml field is set based on the database encoding to handle multi-byte characters correctly
- Error context support allows for graceful error handling in parsing operations
- This structure serves both tsvector and tsquery parsing, making it a unified parsing framework