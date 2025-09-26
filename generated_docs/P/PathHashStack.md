# PathHashStack

## Location
src/backend/utils/adt/jsonb_gin.c: 73 - 77

## Overview
PathHashStack is a simple stack data structure used to maintain a hierarchy of hash values during JSON path-based GIN index key extraction for the jsonb_path_ops operator class.

## Definition


## Detailed Description
PathHashStack implements a lightweight stack structure that tracks hash values at different nesting levels when extracting GIN index keys from JSONB data using the path-based approach. Each stack level represents a nesting level in the JSON structure (objects or arrays), and maintains a cumulative hash that incorporates all parent-level keys leading to the current position.

The structure supports the jsonb_path_ops GIN operator class, which creates hash-based index keys that include the full path context. This allows the index to distinguish between identical values at different JSON paths, such as  versus .

The stack grows and shrinks dynamically as the JSON traversal encounters nested structures, with each level inheriting hash values from its parent to ensure that nested values include the complete path context in their final hash computation.

## Parameters / Member Variables
- hash: hash table empty: Current cumulative hash value that incorporates all parent keys plus any current key context
- : Pointer to the parent stack level, forming a linked list structure for the stack

## Dependencies
- Functions called/Symbols referenced:
  - (This struct is primarily used as a data container)
- Called from (representative examples):
  - gin_extract_jsonb_path (primary usage context)

## Notes and Other Information
- The stack is implemented as a simple linked list with dynamic allocation using palloc() for new levels
- The root level typically starts with hash = 0 and parent = NULL
- Stack levels are pushed for WJB_BEGIN_ARRAY and WJB_BEGIN_OBJECT tokens, and popped for corresponding WJB_END_* tokens
- Hash values are propagated from parent to child levels to ensure complete path context is preserved
- Memory management follows PostgreSQL conventions with pfree() calls when popping stack levels