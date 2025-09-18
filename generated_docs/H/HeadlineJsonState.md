# HeadlineJsonState

## Location
src/backend/tsearch/wparser.c: 34 - 42

## Overview
HeadlineJsonState is a struct that maintains state information for JSON text search headline generation functions in PostgreSQL's full-text search system.

## Definition
```c
typedef struct HeadlineJsonState
{
    HeadlineParsedText *prs;
    TSConfigCacheEntry *cfg;
    TSParserCacheEntry *prsobj;
    TSQuery        query;
    List          *prsoptions;
    bool          transformed;
} HeadlineJsonState;
```

## Detailed Description
HeadlineJsonState serves as a state container for the JSON headline generation process in PostgreSQL's text search functionality. It encapsulates all the necessary components required to process JSON documents and generate highlighted text snippets based on full-text search queries. This struct is specifically designed for the `ts_headline_json_*` family of functions and maintains references to parsing structures, configuration entries, and query information needed throughout the headline generation process.

## Parameters / Member Variables
- `prs`: Pointer to HeadlineParsedText structure containing the parsed text data for headline generation
- `cfg`: Pointer to TSConfigCacheEntry containing cached text search configuration information
- `prsobj`: Pointer to TSParserCacheEntry containing cached parser object for text processing
- `query`: TSQuery object representing the text search query used for highlighting
- `prsoptions`: List of parser options that control the parsing behavior
- `transformed`: Boolean flag indicating whether the text has been transformed during processing

## Dependencies
- Functions called/Symbols referenced:
  - HeadlineParsedText
  - TSConfigCacheEntry
  - TSParserCacheEntry
  - TSQuery
- Called from (representative examples):
  - ts_headline_jsonb_byid_opt
  - ts_headline_json_byid_opt
  - headline_json_value

## Notes and Other Information
This struct is specifically designed for JSON headline generation and is part of PostgreSQL's full-text search infrastructure. It provides a centralized way to maintain state across multiple function calls during the JSON text processing and highlighting workflow. The struct is used internally by the text search system and is not directly exposed to end users.