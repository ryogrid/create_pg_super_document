# pg_parse_json_or_errsave

## Location
src/backend/utils/adt/jsonfuncs.c: 517 - 537

## Overview
This function provides error-safe JSON parsing by extending pg_parse_json with comprehensive error handling through PostgreSQL's ErrorSaveContext mechanism.

## Definition


## Detailed Description
pg_parse_json_or_errsave serves as a wrapper around pg_parse_json that provides enhanced error handling capabilities. Unlike pg_parse_json which returns a JsonParseErrorType, this function returns a boolean success indicator and handles errors through PostgreSQL's error context system. When parsing fails, it either saves error data into the provided ErrorSaveContext (allowing soft error handling) or reports an ERROR (causing transaction abort). This design enables callers to choose between immediate error reporting and deferred error handling based on their error management strategy.

## Parameters / Member Variables
- : JsonLexContext pointer containing the JSON input data and lexical analysis state
- : JsonSemAction pointer defining the semantic actions to execute during JSON parsing
- : Node pointer that may be an ErrorSaveContext for soft error handling, or NULL for immediate error reporting

## Dependencies
- Functions called/Symbols referenced:
  - pg_parse_json
  - json_errsave_error
  - JsonParseErrorType
  - JSON_SUCCESS
- Called from (representative examples):
  - json_in
  - jsonb_from_cstring
  - populate_array_json
  - get_json_object_as_hash

## Notes and Other Information
This function is essential for PostgreSQL's JSON processing infrastructure where error recovery is critical. It enables parsing operations to continue even when individual JSON values are malformed, which is particularly important in bulk data processing scenarios. The function follows PostgreSQL's standard error handling patterns by supporting both immediate error reporting and soft error collection through the ErrorSaveContext mechanism.