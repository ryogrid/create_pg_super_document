# freeJsonLexContext

## Location
[src/common/jsonapi.c:483-521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L483-L521)

## Overview
Frees memory allocated for a JsonLexContext structure and its associated components.

## Definition
```c
void freeJsonLexContext(JsonLexContext *lex)
```

## Detailed Description
The `freeJsonLexContext` function is responsible for properly cleaning up and freeing all memory allocated for a JsonLexContext structure. It handles the deallocation of various components based on flags and the context type (incremental vs non-incremental parsing). The function checks specific flags to determine which components need to be freed, ensuring proper memory management without double-freeing resources. It is particularly important for avoiding memory leaks in JSON parsing operations.

## Parameters / Member Variables
- `lex`: Pointer to the JsonLexContext structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md) (structure type)
  - JSONLEX_FREE_STRVAL (flag constant)
  - [destroyStringInfo](../d/destroyStringInfo.md)
  - JSONLEX_FREE_STRUCT (flag constant)
  - JsonParseErrorType
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
- Called from (representative examples):
  - [json_validate](../j/json_validate.md)
  - [datum_to_jsonb_internal](../d/datum_to_jsonb_internal.md)
  - [json_object_keys](../j/json_object_keys.md)
  - [get_worker](../g/get_worker.md)
  - [each_worker](../e/each_worker.md)
  - [elements_worker](../e/elements_worker.md)
  - [populate_array_json](../p/populate_array_json.md)
  - [get_json_object_as_hash](../g/get_json_object_as_hash.md)
  - [populate_recordset_worker](../p/populate_recordset_worker.md)
  - [iterate_json_values](../i/iterate_json_values.md)
  - [transform_json_string_values](../t/transform_json_string_values.md)
  - [json_parse_manifest_incremental_shutdown](../j/json_parse_manifest_incremental_shutdown.md)
  - [json_parse_manifest](../j/json_parse_manifest.md)

## Notes and Other Information
The function includes detailed logic for different cleanup scenarios:
- Only frees `strval` if the JSONLEX_FREE_STRVAL flag is set
- Only frees the main structure if the JSONLEX_FREE_STRUCT flag is set
- For incremental parsing contexts, it specifically frees additional components like `inc_state`, `partial_token.data`, `pstack` components (`prediction`, `fnames`, `fnull`)
- The function includes a comment noting that cleanup may not be needed in certain scenarios, such as when a lex pointer was provided during object creation, need_escapes was false, json_errdetail() was not called, or when a memory context delete/reset is imminent in the backend environment.