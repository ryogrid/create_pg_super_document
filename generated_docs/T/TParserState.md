# TParserState

## Location
[src/backend/tsearch/wparser_def.c:199-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L199-L216)

## Overview
TParserState is an enumerated type that defines all possible parser states used in PostgreSQL's text search parser for tokenizing input text and identifying different types of tokens.

## Definition


## Detailed Description
TParserState is a comprehensive enumeration that represents all the different states that the PostgreSQL text search parser can be in while processing input text. The parser uses a finite state machine approach to tokenize text, where each state represents a specific context or type of token being parsed. The states cover various token types including:

- Basic token types (words, numbers, spaces)
- Numeric formats (integers, decimals, scientific notation, version numbers)
- XML/HTML constructs (tags, entities, comments)
- URL components (protocols, hosts, paths, ports)
- Email addresses and file paths
- Hyphenated words and compound tokens

The parser transitions between these states based on the input characters and the current parsing context, allowing it to accurately identify and classify different types of textual content for full-text search indexing.

## Parameters / Member Variables
Key state categories:
- : Initial/base state (value 0)
- : Processing numeric words
- : Processing ASCII words
- : Processing general words
- : Processing unsigned integers
- : Processing signed integers
- : Processing whitespace
- : Processing unsigned decimal numbers
- : Processing decimal numbers
- : Processing version numbers
- : Processing scientific notation mantissa
- : Processing XML entities
- : Processing HTML/XML tags
- : Processing HTML/XML comments
- : Processing host names and domains
- : Processing port numbers
- : Processing email addresses
- : Processing file paths
- : Processing URL components
- : Processing protocol identifiers
- : Processing hyphenated words and compounds
- : Sentinel value marking end of enum

## Dependencies
- Functions called/Symbols referenced:
  - [TParser](TParser.md) (struct reference)
- Called from (representative examples):
  - TParserStateActionItem (struct member)
  - [TParserPosition](TParserPosition.md) (struct member)
  - [p_isspecial](../p/p_isspecial.md) (function usage)

## Notes and Other Information
- The order of enum values must match the corresponding state action tables used by the parser
- TPS_Null serves as a sentinel value and is not a valid parsing state
- This enumeration is central to PostgreSQL's full-text search tokenization process
- The state machine design allows for complex parsing rules while maintaining efficient performance
- Each state typically corresponds to specific character classification rules and transition logic