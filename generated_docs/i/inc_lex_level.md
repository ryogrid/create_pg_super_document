# inc_lex_level

## Location
[src/common/jsonapi.c:398-418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L398-L418)

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

## Simplified Source

```c
static inline void inc_lex_level(JsonLexContext *lex) {
    // Increment nesting level
    lex->lex_level += 1;

    // For incremental parsing, expand stacks if needed
    if (lex->incremental && lex->lex_level >= lex->pstack->stack_size) {
        // Expand stack by a chunk
        lex->pstack->stack_size += JS_STACK_CHUNK_SIZE;

        // Reallocate prediction buffer
        lex->pstack->prediction = repalloc(lex->pstack->prediction,
                                         lex->pstack->stack_size * JS_MAX_PROD_LEN);

        // Reallocate field name array if it exists
        if (lex->pstack->fnames)
            lex->pstack->fnames = repalloc(lex->pstack->fnames,
                                         lex->pstack->stack_size * sizeof(char *));

        // Reallocate null flag array if it exists
        if (lex->pstack->fnull)
            lex->pstack->fnull = repalloc(lex->pstack->fnull,
                                        lex->pstack->stack_size * sizeof(bool));
    }
}
```