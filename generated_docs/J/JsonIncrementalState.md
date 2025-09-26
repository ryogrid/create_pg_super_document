# JsonIncrementalState

## Location
src/common/jsonapi.c: 102 - 111

## Overview
JsonIncrementalState is a structure that maintains state information for handling partial tokens at the end of JSON chunks during incremental parsing operations.

## Definition
```c
struct JsonIncrementalState
{
    bool            is_last_chunk;
    bool            partial_completed;
    StringInfoData  partial_token;
};
```

## Detailed Description
JsonIncrementalState is used in PostgreSQL's incremental JSON parsing to handle cases where a JSON token may be split across chunk boundaries. When parsing large JSON documents in chunks, a token (like a string literal or number) might begin at the end of one chunk and continue into the next chunk. This structure maintains the necessary state to reconstruct such partial tokens across parsing calls, ensuring that the parser can correctly handle arbitrarily large JSON inputs without requiring the entire document to be loaded into memory at once.

## Parameters / Member Variables
- `is_last_chunk`: Boolean flag indicating whether the current chunk being processed is the final chunk in the JSON document
- `partial_completed`: Boolean flag indicating whether a partial token that was being reconstructed has been completed
- `partial_token`: StringInfoData structure that accumulates the partial token data across chunk boundaries

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - makeJsonLexContextIncremental
  - pg_parse_json
  - JsonLexContext (as a member)

## Notes and Other Information
The typedef for this structure appears in jsonapi.h, making it available throughout the PostgreSQL codebase. This structure is crucial for streaming JSON parsing where the input may be too large to fit in memory or arrives in chunks over a network connection. The StringInfoData member uses PostgreSQL's dynamic string buffer implementation to efficiently accumulate partial token data.