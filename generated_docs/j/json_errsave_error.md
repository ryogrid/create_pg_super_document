# json_errsave_error

## Location
src/backend/utils/adt/jsonfuncs.c: 639 - 674

## Overview
This function provides centralized error reporting for JSON parsing failures, handling different error types with appropriate error codes and context information.

## Definition
void json_errsave_error(JsonParseErrorType error, JsonLexContext *lex, Node *escontext)

## Detailed Description
json_errsave_error serves as the central error reporting mechanism for PostgreSQL's JSON parsing infrastructure. It categorizes different JSON parsing errors and generates appropriate PostgreSQL error messages with proper error codes and contextual information. The function handles three main categories of errors: Unicode-related errors (escape sequences, untranslatable characters, null code points), semantic action failures, and general syntax errors. For each category, it generates specific error messages and uses the ErrorSaveContext mechanism to either save error information for later processing or immediately report errors. The function leverages report_json_context to provide detailed context about where the error occurred in the input.

## Parameters / Member Variables
- `error`: JsonParseErrorType indicating the specific type of parsing error encountered
- `lex`: JsonLexContext containing the parsing state and input location where the error occurred
- `escontext`: Node pointer for error context handling (ErrorSaveContext for soft errors, NULL for immediate reporting)

## Dependencies
- Functions called/Symbols referenced:
  - errsave
  - errdetail_internal
  - json_errdetail
  - report_json_context
  - SOFT_ERROR_OCCURRED
  - JSON_UNICODE_HIGH_ESCAPE
  - JSON_UNICODE_UNTRANSLATABLE
  - JSON_UNICODE_CODE_POINT_ZERO
  - JSON_SEM_ACTION_FAILED
- Called from (representative examples):
  - pg_parse_json_or_errsave
  - json_validate
  - json_typeof
  - get_array_start
  - json_get_first_token

## Notes and Other Information
This function is critical for maintaining consistent error reporting across PostgreSQL's JSON functionality. It ensures that JSON parsing errors are properly categorized with appropriate SQL error codes (ERRCODE_UNTRANSLATABLE_CHARACTER for Unicode issues, ERRCODE_INVALID_TEXT_REPRESENTATION for syntax errors) and provides detailed context to help users identify and fix JSON input problems. The function's integration with PostgreSQL's error handling framework allows for both immediate error reporting and deferred error collection in bulk processing scenarios.