# JsonUniqueHashEntry

## Location
[src/backend/utils/adt/json.c:42-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L42-L47)

## Overview
JsonUniqueHashEntry is a structure that represents individual hash table entries used to store JSON object key information for fast duplicate key detection in PostgreSQL's JSON processing system.

## Definition

```c
typedef struct JsonUniqueHashEntry
{
	const char *key;
	int			key_len;
	int			object_id;
} JsonUniqueHashEntry;
```
## Detailed Description
JsonUniqueHashEntry serves as the fundamental data storage unit within the JsonUniqueCheckState hash table. Each entry represents a single JSON object key along with its metadata, enabling efficient duplicate key detection during JSON parsing and manipulation operations. The structure is designed to work seamlessly with PostgreSQL's hash table infrastructure (HTAB) to provide O(1) average-case lookup performance for key uniqueness validation.

The structure stores both the key string and its length for efficient comparison operations, while the object_id field enables hierarchical key checking in nested JSON objects, allowing the system to maintain separate key namespaces for different object scopes.

## Parameters / Member Variables
- `*key`: Pointer to the JSON object key string (null-terminated C string)
- `key_len`: Length of the key string in bytes, enabling efficient string operations without strlen() calls
- `object_id`: Integer identifier for the JSON object scope, allowing nested objects to maintain separate key namespaces
## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a simple data structure)
- Called from (representative examples):
  - [json_unique_hash](../j/json_unique_hash.md) (hash function)
  - [json_unique_hash_match](../j/json_unique_hash_match.md) (comparison function)
  - [json_unique_check_init](../j/json_unique_check_init.md) (hash table initialization)
  - [json_unique_check_key](../j/json_unique_check_key.md) (key validation)

## Notes and Other Information
- Used exclusively within PostgreSQL's JSON key uniqueness checking system
- Designed to work with PostgreSQL's HTAB hash table implementation
- The const char *key pointer typically points to string data managed elsewhere (not owned by this structure)
- The object_id enables support for nested JSON objects with independent key namespaces
- Memory management for the key string is handled by the calling context, not by this structure
- Essential component for maintaining JSON specification compliance regarding unique object keys