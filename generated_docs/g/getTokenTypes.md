# getTokenTypes

## Location
src/backend/commands/tsearchcmds.c: 1229 - 1287

## Overview
A static function that translates a list of token type names into a list of unique TSTokenTypeItem structures, validating token names against a text search parser's lexical types.

## Definition


## Detailed Description
This function converts string-based token type names into structured TSTokenTypeItem objects by looking up their corresponding lexical identifiers from a text search parser. It first retrieves the parser's cache entry and calls the parser's lextype method to get the available token types. For each input token name, it searches through the parser's lexical descriptor list to find matching aliases. The function automatically removes duplicates by using tstoken_list_member to check if a token is already in the result list. If a token name is not found in the parser's lexical types, it raises an error with ERRCODE_INVALID_PARAMETER_VALUE.

## Parameters / Member Variables
- : Object ID of the text search parser to use for token type validation
- : List of String nodes containing token type names to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_ts_parser_cache](../l/lookup_ts_parser_cache.md)
  - TSParserCacheEntry
  - LexDescr
  - OidFunctionCall1
  - String
  - [tstoken_list_member](../t/tstoken_list_member.md)
  - TSTokenTypeItem
  - list_length, lfirst_node, lappend, palloc0, pstrdup, strVal
- Called from (representative examples):
  - [MakeConfigurationMapping](../M/MakeConfigurationMapping.md)
  - [DropConfigurationMapping](../D/DropConfigurationMapping.md)

## Notes and Other Information
- This is a static function, only accessible within the tsearchcmds.c file
- Returns NIL (empty list) if the input tokennames list is empty
- Validates that the parser has a defined lextype method before proceeding
- Automatically deduplicates token names to ensure each token type appears only once in the result
- Memory allocation for TSTokenTypeItem structures uses palloc0 for zero-initialized memory
- Token name strings are duplicated using pstrdup to ensure proper memory management
- Part of PostgreSQL's text search configuration management system for mapping token types