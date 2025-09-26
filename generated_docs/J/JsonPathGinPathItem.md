# JsonPathGinPathItem

## Location
[src/backend/utils/adt/jsonb_gin.c:117-122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L117-L122)

## Overview
JsonPathGinPathItem represents a single element in a JSON path for the jsonb_ops GIN indexing strategy, storing path component information including key names and path item types.

## Definition

```c
typedef struct JsonPathGinPathItem
{
	struct JsonPathGinPathItem *parent;
	Datum		keyName;		/* key name (for '.key' path item) or NULL */
	JsonPathItemType type;		/* type of jsonpath item */
} JsonPathGinPathItem;
```
## Detailed Description
JsonPathGinPathItem forms part of a linked list structure that represents JSON path expressions for the jsonb_ops GIN operator class. Each node in the list corresponds to a single path component such as '.key', '.*', '.**', '[index]', or '[*]'.

The structure maintains a parent pointer to build a reverse-linked list where newer path items point to their predecessors. This allows the path to be built incrementally as JSON path expressions are parsed and processed.

The keyName field stores the actual key data for specific key lookups (jpiKey type), while generic path operations like '.*' or '[*]' store NULL. The type field indicates what kind of JSON path operation this item represents, using the JsonPathItemType enumeration.

This structure is specifically designed for the jsonb_ops indexing approach, where individual path components are indexed separately, as opposed to jsonb_path_ops which uses path-aware hashing.

## Parameters / Member Variables
- : Pointer to the parent (previous) path item in the linked list structure
- : Datum containing the key name for '.key' path items, or NULL for generic operations
- : JsonPathItemType enumeration value indicating the type of path operation (jpiKey, jpiAny, jpiAnyKey, jpiAnyArray, jpiIndexArray, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - JsonPathItemType (enumeration from jsonpath system)
  - Datum (PostgreSQL generic data type)
- Called from (representative examples):
  - [jsonb_ops__add_path_item](../j/jsonb_ops__add_path_item.md) (creates and links path items)
  - JsonPathGinPath (union that contains path item lists)
  - [jsonb_ops__extract_nodes](../j/jsonb_ops__extract_nodes.md) (processes path item lists)

## Notes and Other Information
- Used exclusively with jsonb_ops indexing strategy (not jsonb_path_ops)
- Forms a reverse-linked list where newest items point to older parents
- Supports key-specific lookups ('.key') and generic operations ('.*', '[*]', etc.)
- [Path](../P/Path.md) items like methods and complex expressions may not be supported and return false from add_path_item
- Memory allocation uses palloc for individual path item nodes
- The keyName datum is created using make_text_key() for actual key strings and PointerGetDatum(NULL) for wildcard operations