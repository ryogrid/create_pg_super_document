# report_json_context

## Location
[src/backend/utils/adt/jsonfuncs.c:675-729](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L675-L729)

## Overview
This static function generates contextual error information for JSON parsing failures by creating formatted context lines that show the problematic input location.

## Definition
static int report_json_context(JsonLexContext *lex)

## Detailed Description
report_json_context creates detailed context information for JSON error reporting by extracting and formatting the relevant portion of the input text surrounding the error location. The function intelligently determines the appropriate context boundaries, typically showing up to 50 characters leading up to the error position while respecting multibyte character boundaries. It handles display formatting by adding ellipsis indicators when the context excerpt doesn't represent the complete line, and provides line number information to help users locate the problematic JSON input. The function is designed to be called within ereport() contexts and returns an integer (though the return value is not meaningful) to conform to PostgreSQL's error reporting conventions.

## Parameters / Member Variables
- `lex`: JsonLexContext containing the parsing state, input text, current position, and line information for context generation

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET
  - [pg_mblen](../p/pg_mblen.md)
  - [palloc](../p/palloc.md)
  - memcpy
  - errcontext
  - JSON_TOKEN_END
- Called from (representative examples):
  - [json_errsave_error](../j/json_errsave_error.md) (multiple call sites)
  - JsObjectFree

## Notes and Other Information
This function is essential for providing user-friendly JSON error messages by showing the exact location and context where parsing failed. The implementation carefully handles multibyte characters to avoid corrupting the display of non-ASCII text, and uses intelligent truncation to keep error messages readable while providing sufficient context. The function's static scope indicates it's an internal utility specifically designed to support the JSON error reporting infrastructure. The context formatting follows PostgreSQL's standard error message conventions, making JSON errors consistent with other database error types.