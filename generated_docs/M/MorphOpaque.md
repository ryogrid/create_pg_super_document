# MorphOpaque

## Location
[src/backend/tsearch/to_tsany.c:25-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L25-L36)

## Overview
MorphOpaque is an opaque data structure used as a communication mechanism between parse_tsquery() and pushval_morph() functions in PostgreSQL's full-text search morphological parsing system.

## Definition


## Detailed Description
MorphOpaque serves as a container for configuration data that needs to be passed between the tsquery parsing functions. It encapsulates the text search configuration ID and the operator used to connect multiple words that result from parsing a single tsquery morph. The structure is designed to be opaque to the calling code, hiding implementation details while providing necessary context for morphological parsing operations.

When a single tsquery morph is parsed into multiple words that reside in adjacent positions, they need to be connected using a specific operator. This structure ensures that the correct operator (typically OP_PHRASE) is used to maintain the semantic relationship between the parsed words, requiring that word positions in the complex morph exactly match those in the tsvector.

## Parameters / Member Variables
- : Object identifier (Oid) of the text search configuration to be used for morphological parsing
- : Integer representing the operator used to connect multiple words from a single morph when they are in adjacent positions (usually OP_PHRASE)

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [pushval_morph](../p/pushval_morph.md)
  - [to_tsquery_byid](../t/to_tsquery_byid.md)
  - [plainto_tsquery_byid](../p/plainto_tsquery_byid.md)
  - [phraseto_tsquery_byid](../p/phraseto_tsquery_byid.md)  
  - [websearch_to_tsquery_byid](../w/websearch_to_tsquery_byid.md)

## Notes and Other Information
- The structure is specifically designed for internal use within the tsquery morphological parsing system
- The qoperator field typically contains OP_PHRASE to ensure exact positional matching between parsed morphs and tsvector content
- This structure facilitates the separation of concerns between query parsing logic and morphological analysis
- Located in src/backend/tsearch/to_tsany.c:25-36