# escapify

## Location
[src/test/modules/test_escape/test_escape.c:100-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_escape/test_escape.c#L100-L118)

## Overview
A utility function that converts a string into a more readable format by escaping non-printable and non-ASCII characters for debugging and testing purposes.

## Definition

```c
static void
escapify(PQExpBuffer buf, const char *str, size_t len)
```
## Detailed Description
The  function processes a string character by character and appends an escaped representation to a PQExpBuffer. It makes characters outside of plain ASCII more recognizable by converting them to escape sequences. This function is primarily used in PostgreSQL's test modules for displaying string content in a human-readable format during testing and debugging.

The function handles special characters as follows:
- Newline characters () are converted to the literal string 
- Null characters () are converted to the literal string 
- Characters outside the printable ASCII range (< ' ' or > '~') are converted to hexadecimal escape sequences ()
- All other characters are appended as-is

## Parameters / Member Variables
- `buf`: PQExpBuffer where the escaped string will be appended
- `*str`: Source string to be escaped
- `len`: Length of the source string in bytes
## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
- Called from (representative examples):
  - [test_gb18030_page_multiple](../t/test_gb18030_page_multiple.md)
  - [test_gb18030_json](../t/test_gb18030_json.md)
  - [test_psql_parse](../t/test_psql_parse.md)
  - [test_one_vector_escape](../t/test_one_vector_escape.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the test_escape.c file
- The output format is noted in the comments as potentially improvable and not completely unambiguous
- Used extensively in PostgreSQL's escape-related test modules for displaying test data and results
- Part of the test infrastructure rather than core PostgreSQL functionality