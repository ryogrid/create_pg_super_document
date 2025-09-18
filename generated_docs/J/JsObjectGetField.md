# JsObjectGetField

## Location
src/backend/utils/adt/jsonfuncs.c: 3490 - 3517

## Overview
Retrieves a specific field value from a JsObject structure, handling both JSON text (via hash table) and binary JSONB (via container lookup) formats.

## Definition
```c
static bool JsObjectGetField(JsObject *obj, char *field, JsValue *jsv)
```

## Detailed Description
This function provides a unified interface for field lookup in JSON objects regardless of whether they are stored as parsed JSON text (using hash tables) or as binary JSONB (using containers). For JSON text objects, it performs a hash table search to find the field entry and sets up the JsValue with appropriate JSON token information. For JSONB objects, it uses the container-based lookup function to find the field value. The function returns a boolean indicating whether the field was found, while populating the provided JsValue structure with the result.

## Parameters / Member Variables
- `obj`: Pointer to JsObject containing the JSON object data (either hash table or JSONB container)
- `field`: Null-terminated string containing the field name to search for
- `jsv`: Pointer to JsValue structure to be populated with the field value result

## Dependencies
- Functions called/Symbols referenced:
  - hash_search
  - getKeyJsonValueFromContainer
  - strlen
- Called from (representative examples):
  - JsObjectFree
  - populate_record

## Notes and Other Information
This function abstracts the differences between JSON text and binary JSONB field access, providing a consistent interface for JSON object field retrieval. For JSON text, it leverages PostgreSQL's hash table implementation for efficient field lookup. For JSONB, it uses the specialized container functions that work with the binary format. The function properly handles null/missing fields by setting appropriate null indicators and return values. The field length for JSON text is set to -1 to indicate null-terminated strings, while JSONB values are handled through their container pointers. This abstraction is crucial for the JSON population functions that need to work with both JSON formats uniformly.