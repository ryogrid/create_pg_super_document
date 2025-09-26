# JsonUniqueStackEntry

## Location
[src/backend/utils/adt/json.c:50-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L50-L54)

## Overview
JsonUniqueStackEntry is a structure that represents a stack element used to track nested JSON object scopes during JSON parsing for hierarchical key uniqueness checking.

## Definition

```c
typedef struct JsonUniqueStackEntry
{
	struct JsonUniqueStackEntry *parent;
	int			object_id;
} JsonUniqueStackEntry;
```
## Detailed Description
JsonUniqueStackEntry implements a linked-list-based stack structure that maintains the hierarchical context of nested JSON objects during parsing operations. Each stack entry represents a single JSON object scope level, enabling the system to maintain separate key namespaces for different nesting levels while ensuring key uniqueness within each individual object.

The structure supports the recursive nature of JSON by providing a parent pointer that creates a chain back to outer object scopes. This design enables efficient push/pop operations when entering and exiting JSON object boundaries during parsing, while the object_id field provides a unique identifier for each object scope level.

## Parameters / Member Variables
- : Pointer to the parent stack entry, creating a linked-list chain representing the nesting hierarchy (NULL for the root level)
- : Unique integer identifier for the current JSON object scope, used to associate keys with their containing object

## Dependencies
- Functions called/Symbols referenced:
  - [JsonUniqueStackEntry](JsonUniqueStackEntry.md) (self-reference for parent pointer)
- Called from (representative examples):
  - [json_unique_object_start](../j/json_unique_object_start.md) (when entering a new JSON object)
  - [json_unique_object_end](../j/json_unique_object_end.md) (when exiting a JSON object)  
  - [json_unique_object_field_start](../j/json_unique_object_field_start.md) (when processing object fields)
  - [JsonUniqueParsingState](JsonUniqueParsingState.md) (as member variable)

## Notes and Other Information
- Forms a runtime stack that mirrors the nested structure of JSON objects being parsed
- Each entry corresponds to one level of JSON object nesting
- The parent pointer creates a singly-linked list from current scope back to root
- Object IDs are typically assigned incrementally to ensure uniqueness across the entire JSON document
- Memory management follows a stack discipline - entries are allocated when entering objects and freed when exiting
- Essential for maintaining proper key uniqueness semantics in nested JSON structures
- Works in conjunction with JsonUniqueHashEntry to provide complete duplicate key detection