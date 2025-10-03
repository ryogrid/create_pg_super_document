# jsonb_path_ops__add_path_item

## Location
[src/backend/utils/adt/jsonb_gin.c:323-352](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L323-L352)

## Overview
Combines an existing path hash with the next key hash for the jsonb_path_ops GIN index operator class, used to track JSON path navigation for path-aware indexing.

## Definition

```c
static bool
jsonb_path_ops__add_path_item(JsonPathGinPath *path, JsonPathItem *jsp)
```
## Detailed Description
This function is a core component of PostgreSQL's JSONB GIN indexing system for path-aware operations. It processes individual JSON path items and updates the cumulative hash value stored in the JsonPathGinPath structure. The function supports specific path item types that are meaningful for path-based indexing:

- **jpiRoot**: Resets the path hash to 0, representing the start of a new path
- **jpiKey**: Extracts the string key and combines it with the existing hash using JsonbHashScalarValue
- **jpiIndexArray/jpiAnyArray**: Leaves the path hash unchanged, allowing array navigation without affecting the path signature

The function returns false for unsupported path item types (wildcards, item methods), indicating that the path cannot be efficiently indexed using the path_ops strategy.

## Parameters / Member Variables
- `*path`: Pointer to JsonPathGinPath structure containing the cumulative hash value being built
- `*jsp`: Pointer to JsonPathItem representing the current path element to be processed
## Dependencies
- Functions called/Symbols referenced:
  - [jspGetString](jspGetString.md) (extracts string value from JSON path item)
  - [JsonbHashScalarValue](../J/JsonbHashScalarValue.md) (computes hash for JSONB scalar values)
  - JsonPathGinPath (path tracking structure)
  - JsonPathItem (individual path element structure)
  - JSON path item type constants (jpiRoot, jpiKey, jpiIndexArray, jpiAnyArray)
  - [JsonbValue](../J/JsonbValue.md) and jbvString (JSONB value representation)

- Called from:
  - [extract_jsp_query](../e/extract_jsp_query.md) (main query extraction function for JSONB GIN indexing)

## Notes and Other Information
This function is specifically designed for the jsonb_path_ops GIN operator class, which provides path-aware indexing capabilities. It only supports a subset of JSON path operations that can be efficiently indexed. Complex path expressions with wildcards or method calls are not supported and will cause the function to return false, indicating the query cannot be optimized using this index strategy.