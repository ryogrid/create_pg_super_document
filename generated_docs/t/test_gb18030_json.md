# test_gb18030_json

## Location
src/test/modules/test_escape/test_escape.c: 213 - 250

## Overview
A specialized test function that verifies memory boundary safety when PostgreSQL's JSON parser processes invalid GB18030-encoded Unicode escape sequences at memory allocation boundaries.

## Definition
```c
static void
test_gb18030_json(pe_test_config *tc)
```

## Detailed Description
The `test_gb18030_json` function tests the memory safety of PostgreSQL's JSON parsing infrastructure when handling malformed Unicode escape sequences in GB18030 encoding. This test is specifically designed to ensure that the JSON parser doesn't read past allocated memory boundaries when encountering invalid input.

The function creates a crafted JSON input string containing an incomplete Unicode escape sequence ("{\"\\u\xFE") that ends with an invalid GB18030 byte. The test uses Valgrind memory access controls to make the memory beyond the input buffer inaccessible, ensuring that any attempt to read past the buffer boundary would be detected.

The test exercises the same wide-character infrastructure (wchar.c) that the main escape tests use, but focuses specifically on JSON parsing rather than literal escaping. It expects the JSON parser to detect the malformed Unicode escape sequence and return a specific error code (JSON_UNICODE_ESCAPE_FORMAT) rather than crashing or reading invalid memory.

## Parameters / Member Variables
- `tc`: Pointer to pe_test_config structure containing test configuration

## Dependencies
- Functions called/Symbols referenced:
  - [pe_test_config](../p/pe_test_config.md) (struct type)
  - [JsonLexContext](../J/JsonLexContext.md) (JSON lexer context type)
  - [JsonSemAction](../J/JsonSemAction.md) (JSON semantic action structure)
  - JsonParseErrorType (JSON error enumeration)
  - createPQExpBuffer (libpq buffer creation)
  - appendBinaryPQExpBuffer (libpq binary buffer append)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md) (libpq string buffer append)
  - NEVER_ACCESS_STR (constant for Valgrind testing)
  - VALGRIND_MAKE_MEM_NOACCESS (Valgrind memory control macro)
  - [escapify](../e/escapify.md) (local utility function for readable output)
  - [makeJsonLexContextCstringLen](../m/makeJsonLexContextCstringLen.md) (JSON lexer context creation)
  - PG_GB18030 (encoding constant)
  - [pg_parse_json](../p/pg_parse_json.md) (JSON parser function)
  - JSON_UNICODE_ESCAPE_FORMAT (expected error constant)
  - [report_result](../r/report_result.md) (local test reporting function)
  - json_errdetail (JSON error detail function)
  - freeJsonLexContext (JSON lexer cleanup)
  - destroyPQExpBuffer (libpq buffer cleanup)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a static function, accessible only within the test_escape.c file
- Tests JSON parsing infrastructure rather than escape functions, but exercises similar wide-character code paths
- Uses Valgrind memory access controls to detect boundary violations
- The input string "{\"\\u\xFE" represents a JSON object with an incomplete Unicode escape sequence
- Tests specifically with GB18030 encoding, which has complex multi-byte character handling
- Expects the parser to return JSON_UNICODE_ESCAPE_FORMAT error for the malformed Unicode escape
- Part of PostgreSQL's memory safety testing infrastructure for JSON processing
- The test validates that JSON parsing properly handles encoding-specific boundary conditions without memory access violations