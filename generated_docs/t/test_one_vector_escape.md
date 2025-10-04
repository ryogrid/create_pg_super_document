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
- `*tc`: Test configuration structure containing database connection and test settings
- `*tv`: Test vector containing the input data to be escaped and expected encoding information
- `*ef`: Escape function descriptor containing the function pointer and capability flags
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
  - [encoding_conflicts_ascii](../e/encoding_conflicts_ascii.md)
  - [PQclientEncoding](../P/PQclientEncoding.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [appendBinaryPQExpBuffer](../a/appendBinaryPQExpBuffer.md)
  - [escapify](../e/escapify.md)
  - [pg_encoding_verifymbstr](../p/pg_encoding_verifymbstr.md)
  - [strnlen](../s/strnlen.md)
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

## Simplified Source
```c
static void
test_one_vector_escape(pe_test_config *tc, const pe_test_vector *tv, const pe_test_escape_func *ef)
{
    PQExpBuffer testname, details, raw_buf, escape_buf, escape_err;
    bool input_encoding_valid, escape_success;

    /* Initialize buffers */
    escape_err = createPQExpBuffer();
    testname = createPQExpBuffer();
    details = createPQExpBuffer();
    raw_buf = createPQExpBuffer();
    escape_buf = createPQExpBuffer();

    /* Skip test if encoding conflicts with ASCII-only functions */
    if (ef->supports_only_ascii_overlap &&
        encoding_conflicts_ascii(PQclientEncoding(tc->conn)))
        goto out;

    /* Create test name and details */
    appendPQExpBuffer(testname, ">", tv->escape, tv->escape_len);
    escapify(testname, tv->escape, tv->escape_len);
    appendPQExpBuffer(testname, "< - %s - %s", tv->client_encoding, ef->name);

    /* Validate input encoding */
    size_t input_validlen = pg_encoding_verifymbstr(PQclientEncoding(tc->conn),
                                                   tv->escape, tv->escape_len);
    input_encoding_valid = input_validlen == tv->escape_len;

    /* Skip if function only supports valid input and input is invalid */
    if (!input_encoding_valid && ef->supports_only_valid && !tc->force_unsupported)
        goto out;

    /* Prepare input buffer with boundary protection */
    appendBinaryPQExpBuffer(raw_buf, tv->escape, tv->escape_len);
    if (ef->supports_input_length)
        appendPQExpBufferStr(raw_buf, NEVER_ACCESS_STR);
    else {
        appendPQExpBufferChar(raw_buf, 0);
        appendPQExpBufferStr(raw_buf, NEVER_ACCESS_STR);
    }

    /* Test the escape function */
    escape_success = ef->escape(tc->conn, escape_buf,
                               raw_buf->data, tv->escape_len, escape_err);

    /* Validate escape output */
    if (escape_buf->len > 0)
    {
        /* Check encoding validity of escaped output */
        size_t escape_validlen = pg_encoding_verifymbstr(PQclientEncoding(tc->conn),
                                                        escape_buf->data, escape_buf->len);
        bool escape_encoding_valid = escape_validlen == escape_buf->len;

        /* Verify no data beyond input boundary was accessed */
        bool contains_never = strstr(escape_buf->data, NEVER_ACCESS_STR) == NULL;
        report_result(tc, contains_never, testname->data, details->data,
                     "escaped data beyond end of input",
                     contains_never ? "no" : "all secrets revealed");

        /* Test SQL parsing to prevent injection */
        test_psql_parse(tc, testname, escape_buf, details);
    }

    /* Additional validation tests for error reporting and encoding consistency */
    if (ef->reports_errors)
    {
        /* Validate error reporting matches input validity */
        bool ok = (escape_success == input_encoding_valid);
        report_result(tc, ok, testname->data, details->data,
                     "input validity vs escape success", ok ? "ok" : "mismatch");
    }

out:
    /* Cleanup */
    destroyPQExpBuffer(escape_err);
    destroyPQExpBuffer(details);
    destroyPQExpBuffer(testname);
    destroyPQExpBuffer(escape_buf);
    destroyPQExpBuffer(raw_buf);
}
```