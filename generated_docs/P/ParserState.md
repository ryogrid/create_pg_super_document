# ParserState

## Location
[src/test/modules/test_parser/test_parser.c:29-35](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_parser/test_parser.c#L29-L35)

## Overview
ParserState is a structure used in PostgreSQL's test parser module to maintain the state of text parsing operations, tracking the current position within a buffer of text being parsed.

## Definition

```c
typedef struct
{
	int			lexid;
	char	   *alias;
	char	   *descr;
} LexDescr;
```
## Detailed Description
ParserState is a simple state management structure used by PostgreSQL's test text search parser implementation. It serves as a context object that maintains parsing state across multiple function calls during text analysis operations. The structure is designed to support incremental parsing by tracking the current position within a text buffer, allowing the parser to process text token by token while maintaining state between calls.

This structure is part of a test module that demonstrates how to implement a custom text search parser for PostgreSQL's full-text search functionality. The parser distinguishes between word tokens and blank space tokens, advancing through the text buffer character by character.

## Parameters / Member Variables
- `lexid`: Pointer to the text string that is being parsed. This is the input text that the parser will tokenize.
- `*alias`: The total length of the text stored in the buffer, used for bounds checking during parsing.
- `*descr`: The current parsing position within the buffer, indicating where the parser is currently positioned in the text.
## Dependencies
- Functions called/Symbols referenced:
  - (This structure itself doesn't call functions, but serves as a data container)
- Called from (representative examples):
  - [testprs_start](../t/testprs_start.md) (initializes ParserState)
  - [testprs_getlexeme](../t/testprs_getlexeme.md) (uses ParserState to extract tokens)
  - [testprs_end](../t/testprs_end.md) (cleans up ParserState)

## Notes and Other Information
- This structure is allocated using PostgreSQL's palloc0() function in testprs_start() and freed using pfree() in testprs_end()
- The parser implementation is a simple example that only recognizes two token types: words (type 3) and blanks (type 12)
- The structure maintains state between parsing calls, allowing for streaming/incremental parsing of large text inputs
- Located in src/test/modules/test_parser/test_parser.c:24-29
- Part of PostgreSQL's test suite demonstrating text search parser extension capabilities