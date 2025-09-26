# JsonParserStack

## Location
src/common/jsonapi.c: 84 - 101

## Overview
JsonParserStack is a structure containing the three stacks used in non-recursive JSON parsing, along with token and value storage for scalars that need to be preserved across parsing calls.

## Definition


## Detailed Description
JsonParserStack is a core data structure used in PostgreSQL's non-recursive JSON parsing implementation. It maintains the parsing state across multiple calls to incremental parsing functions, enabling the parser to handle large JSON documents without deep recursion that could cause stack overflow. The structure contains three main stacks: prediction stack for tracking parsing expectations, field name stack, and null indicator stack, along with storage for scalar tokens and values that span parsing boundaries.

## Parameters / Member Variables
- : The allocated size of the various stacks within the structure
- : Stack used for tracking parsing predictions and expectations during non-recursive parsing
- : Current index position within the prediction stack
- : Array of field name strings indexed by lexical level, used to track nested object field names
- : Array of boolean flags indexed by lexical level, indicating null values for corresponding field names
- : Token type for scalar values that need to be preserved across parsing calls
- : String value for scalars that need to be preserved across parsing calls

## Dependencies
- Functions called/Symbols referenced:
  - JsonTokenType
- Called from (representative examples):
  - makeJsonLexContextIncremental
  - push_prediction
  - pop_prediction
  - next_prediction
  - have_prediction
  - pg_parse_json
  - pg_parse_json_incremental

## Notes and Other Information
The typedef for this structure appears in jsonapi.h, making it available throughout the PostgreSQL codebase. This structure is essential for incremental JSON parsing, allowing the parser to maintain state between calls and handle arbitrarily large JSON documents without risking stack overflow from deep recursion.