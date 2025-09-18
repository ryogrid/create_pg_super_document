# inc_lex_level

## Location
src/common/jsonapi.c: 398 - 418

## Overview
A static inline function that increments the lexical nesting level in a JSON lexer context and dynamically expands parser stack storage when needed for incremental parsing.

## Definition
static inline void inc_lex_level(JsonLexContext *lex)

## Detailed Description
The inc_lex_level function manages the nesting depth tracking in JSON parsing and handles dynamic memory allocation for incremental parsing contexts. It first increments the lex_level counter to track how deeply nested the parser currently is in JSON structures (objects and arrays). For incremental parsing mode, when the nesting level approaches the current stack size limit, the function proactively expands the prediction stack and associated arrays (field names and null flags) to accommodate deeper nesting. This prevents stack overflow and ensures the incremental parser can handle arbitrarily deep JSON structures within memory limits.

## Parameters / Member Variables
- lex: Pointer to the JsonLexContext structure containing the current parser state and incremental parsing stack

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md) (structure access)
  - JS_STACK_CHUNK_SIZE (stack expansion increment constant)
  - [repalloc](../r/repalloc.md) (memory reallocation function)
  - JS_MAX_PROD_LEN (maximum production length constant)
- Called from (representative examples):
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md) (multiple call sites)

## Notes and Other Information
This function is specifically designed for incremental JSON parsing where the parser must maintain state across multiple input chunks. The dynamic stack expansion ensures that deeply nested JSON structures can be parsed without predetermined limits. The function only performs expensive memory reallocation when actually needed (when incremental mode is enabled and the stack is nearly full), making it efficient for both regular and incremental parsing scenarios. The expansion happens in chunks (JS_STACK_CHUNK_SIZE) to amortize allocation costs.