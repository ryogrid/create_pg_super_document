# jsonb_ops__add_path_item

## Location
[src/backend/utils/adt/jsonb_gin.c:278-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L278-L322)

## Overview
A static utility function that appends a JsonPathItem to a JsonPathGinPath structure, building a linked list representation of JSONPath expressions for the jsonb_ops GIN operator class.

## Definition

```c
static bool
jsonb_ops__add_path_item(JsonPathGinPath *path, JsonPathItem *jsp)
```
## Detailed Description
This function processes individual JsonPath items and builds a linked list representation of the path within a JsonPathGinPath structure. It supports a subset of JSONPath operations that are relevant for GIN indexing in the jsonb_ops operator class. The function creates JsonPathGinPathItem nodes and links them together to form a complete path representation.

The function handles several types of path items:
- **Root items (jpiRoot)**: Resets the path by setting items to NULL
- **Key access (jpiKey)**: Creates a key-based path item with the actual key name stored as a Datum
- **Wildcard operations**: Various wildcard patterns (jpiAny, jpiAnyKey, jpiAnyArray, jpiIndexArray) are represented with NULL key names

Unsupported path items (like method calls) cause the function to return false, indicating that the path cannot be processed for this indexing strategy.

## Parameters / Member Variables
- `*path`: Pointer to the JsonPathGinPath structure being built
- `*jsp`: Pointer to the JsonPathItem to be processed and added to the path
## Return Value
- Returns  if the path item was successfully processed and added
- Returns  if the path item type is not supported for jsonb_ops indexing

## Dependencies
- Functions called/Symbols referenced:
  - [jspGetString](jspGetString.md) (extracts string from JsonPathItem)
  - [make_text_key](../m/make_text_key.md) (converts key string to indexable Datum with JGINFLAG_KEY)
  - [palloc](../p/palloc.md) (allocates memory for new path item)
  - [PointerGetDatum](../P/PointerGetDatum.md) (creates NULL Datum for wildcard items)
- Types/Constants referenced:
  - JsonPathGinPath, JsonPathGinPathItem, JsonPathItem
  - JsonPathItemType constants (jpiRoot, jpiKey, jpiAny, etc.)
  - JGINFLAG_KEY (flag indicating key semantics)
- Called from (representative examples):
  - [extract_jsp_query](../e/extract_jsp_query.md) (at src/backend/utils/adt/jsonb_gin.c:766)

## Notes and Other Information
- This is a static function, accessible only within jsonb_gin.c
- Part of the JSONPath support infrastructure for the jsonb_ops GIN operator class
- Builds a reverse-linked list (parent pointers) to represent the path hierarchy
- Memory for each path item is allocated using palloc() and managed by PostgreSQL's memory context system
- The function supports only path operations that can be efficiently indexed, excluding complex operations like method calls
- Key names are stored as Datum values using the make_text_key utility with appropriate flags
- Wildcard operations are represented uniformly with NULL Datum values, allowing the index to handle pattern matching efficiently