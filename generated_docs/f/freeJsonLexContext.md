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
  - JsonLexContext (structure type)
  - JSONLEX_FREE_STRVAL (flag constant)
  - destroyStringInfo
  - JSONLEX_FREE_STRUCT (flag constant)
  - JsonParseErrorType
  - pfree (PostgreSQL memory deallocation function)
- Called from (representative examples):
  - json_validate
  - datum_to_jsonb_internal
  - json_object_keys
  - get_worker
  - each_worker
  - elements_worker
  - populate_array_json
  - get_json_object_as_hash
  - populate_recordset_worker
  - iterate_json_values
  - transform_json_string_values
  - json_parse_manifest_incremental_shutdown
  - json_parse_manifest

## Notes and Other Information
The function includes detailed logic for different cleanup scenarios:
- Only frees `strval` if the JSONLEX_FREE_STRVAL flag is set
- Only frees the main structure if the JSONLEX_FREE_STRUCT flag is set
- For incremental parsing contexts, it specifically frees additional components like `inc_state`, `partial_token.data`, `pstack` components (`prediction`, `fnames`, `fnull`)
- The function includes a comment noting that cleanup may not be needed in certain scenarios, such as when a lex pointer was provided during object creation, need_escapes was false, json_errdetail() was not called, or when a memory context delete/reset is imminent in the backend environment.