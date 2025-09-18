# TSVectorBuildState

## Location
[src/backend/tsearch/to_tsany.c:38-42](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L38-L42)

## Overview
TSVectorBuildState is a state structure used for incrementally building tsvector values from JSON/JSONB data during text search operations.

## Definition


## Detailed Description
TSVectorBuildState serves as a stateful container for building tsvector data from JSON/JSONB elements. It maintains a ParsedText structure that accumulates parsed words and lexemes as JSON elements are processed, along with the text search configuration ID that determines how text parsing should be performed. This structure enables efficient incremental construction of tsvector values when processing complex JSON structures where multiple text elements need to be parsed and combined into a single searchable vector.

The structure is designed to work with callback-based JSON processing, where individual JSON elements are parsed one at a time and their lexemes are accumulated in the ParsedText structure. Between elements, artificial positional breaks are inserted to prevent phrase searches from incorrectly matching words across JSON element boundaries.

## Parameters / Member Variables
- : Pointer to a ParsedText structure that accumulates parsed words and maintains parsing state including word array, counts, and current position
- : Object identifier (Oid) of the text search configuration used for parsing and lexeme extraction

## Dependencies
- Functions called/Symbols referenced:
  - ParsedText (structure containing word parsing state)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [jsonb_to_tsvector_worker](../j/jsonb_to_tsvector_worker.md)
  - [json_to_tsvector_worker](../j/json_to_tsvector_worker.md)
  - [add_to_tsvector](../a/add_to_tsvector.md)

## Notes and Other Information
- Used specifically for JSON/JSONB to tsvector conversion operations
- The ParsedText structure is initialized on first use with a reasonable default size (16 words) and grows as needed
- Artificial position breaks are inserted between JSON elements to maintain proper phrase search semantics
- Part of PostgreSQL's full-text search system for handling structured JSON data
- Located in src/backend/tsearch/to_tsany.c:38-42