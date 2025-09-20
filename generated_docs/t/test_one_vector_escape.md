# test_one_vector_escape

## Location
[src/test/modules/test_escape/test_escape.c:637-865](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_escape/test_escape.c#L637-L865)

## Overview
A comprehensive test function that validates the security and correctness of a specific escape function by testing it against a single test vector, including encoding validation, boundary checking, and SQL injection protection.

## Definition

```c
static void
test_one_vector_escape(pe_test_config *tc, const pe_test_vector *tv, const pe_test_escape_func *ef)
```
## Detailed Description
This function performs extensive validation of an escape function using a specific test vector. It conducts multiple layers of testing including: input encoding validation, escape function execution with boundary protection, output encoding validation, memory access boundary checking, error handling validation, and SQL parsing validation to prevent injection attacks. The function uses Valgrind integration to detect memory access violations and implements sophisticated encoding validation to ensure that invalid input doesn't produce valid output that could be exploited. It also tests psql parsing to ensure the escaped output is interpreted as a single SQL statement.

## Parameters / Member Variables
- : Test configuration structure containing database connection and test settings
- : Test vector containing the input data to be escaped and expected encoding information
- : Escape function descriptor containing the function pointer and capability flags

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - destroyPQExpBuffer
  - [encoding_conflicts_ascii](../e/encoding_conflicts_ascii.md)
  - [PQclientEncoding](../P/PQclientEncoding.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - appendPQExpBufferChar
  - appendBinaryPQExpBuffer
  - [escapify](../e/escapify.md)
  - [pg_encoding_verifymbstr](../p/pg_encoding_verifymbstr.md)
  - strnlen
  - strstr
  - VALGRIND_MAKE_MEM_NOACCESS
  - [report_result](../r/report_result.md)
  - [test_psql_parse](test_psql_parse.md)
- Constants referenced:
  - NEVER_ACCESS_STR
- Types referenced:
  - [pe_test_config](../p/pe_test_config.md)
  - [pe_test_vector](../p/pe_test_vector.md)
  - [pe_test_escape_func](../p/pe_test_escape_func.md)
- Called from (representative examples):
  - [test_one_vector](test_one_vector.md)

## Notes and Other Information
- This is a test module function located in 
- Implements comprehensive security testing including boundary checks and SQL injection prevention
- Uses Valgrind integration to detect memory access violations beyond input boundaries
- Validates encoding consistency between input and output to prevent encoding-based attacks
- Tests multiple scenarios: valid/invalid input encoding, escape success/failure, and output validation
- Includes special handling for ASCII-only escape functions and encoding conflicts
- Uses NEVER_ACCESS_STR pattern to detect unauthorized memory access beyond input boundaries
- Supports both length-aware and null-terminated escape functions through capability flags
- Performs SQL parsing validation to ensure escaped output cannot be used for statement smuggling
- Implements sophisticated error reporting with detailed diagnostic information for debugging
- Key security principle: invalid input encoding should not produce valid output encoding to prevent bypass attacks