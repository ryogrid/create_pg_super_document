# test_gb18030_page_multiple

## Location
[src/test/modules/test_escape/test_escape.c:180-212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_escape/test_escape.c#L180-L212)

## Overview
A specialized test function that verifies memory boundary safety when PostgreSQL's PQescapeLiteral function processes invalid GB18030-encoded input at memory allocation boundaries.

## Definition
```c
static void
test_gb18030_page_multiple(pe_test_config *tc)
```

## Detailed Description
The `test_gb18030_page_multiple` function is designed to test a specific edge case related to memory safety in PostgreSQL's libpq escape functions. It creates a large buffer (128 KiB) ending with an invalid GB18030 byte sequence and tests whether `PQescapeLiteral` safely handles the input without reading past the end of the allocated memory.

The test is specifically designed to detect potential buffer overrun issues that could cause segmentation faults on systems like OpenBSD, where reading past allocated memory boundaries immediately triggers SIGSEGV. The function sets up a controlled scenario where the escape function encounters an incomplete multi-byte character at the very end of a memory page boundary.

The test creates an input buffer filled with ASCII dash characters ('-') and terminates it with byte 0xfe, which is invalid in GB18030 encoding. The function then sets the client encoding to GB18030 and attempts to escape the buffer, expecting the operation to fail gracefully (return NULL) rather than crash.

## Parameters / Member Variables
- `tc`: Pointer to pe_test_config structure containing test configuration and database connection

## Dependencies
- Functions called/Symbols referenced:
  - [pe_test_config](../p/pe_test_config.md) (struct type)
  - [pg_malloc](../p/pg_malloc.md) (PostgreSQL memory allocation)
  - memset (standard library function)
  - [createPQExpBuffer](../c/createPQExpBuffer.md) (libpq buffer creation)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (libpq buffer append)
  - [escapify](../e/escapify.md) (local utility function for readable output)
  - [PQsetClientEncoding](../P/PQsetClientEncoding.md) (libpq encoding setting)
  - [PQescapeLiteral](../P/PQescapeLiteral.md) (libpq escape function being tested)
  - [report_result](../r/report_result.md) (local test reporting function)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md) (libpq buffer cleanup)
  - [pg_free](../p/pg_free.md) (PostgreSQL memory deallocation)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a static function, accessible only within the test_escape.c file
- Designed to run early in the test suite when freelists are minimal to maximize the chance of detecting boundary issues
- Uses a large buffer size (0x20000 = 128 KiB) to increase likelihood of hitting page boundaries
- Tests specifically for GB18030 encoding, which has complex multi-byte character sequences
- The test expects PQescapeLiteral to return NULL for invalid input, indicating graceful failure
- Part of PostgreSQL's memory safety testing infrastructure
- Comments indicate that smaller buffer sizes (4096 bytes) didn't trigger the issue, while 8192 bytes did on the tested system

## Simplified Source

```c
static void
test_gb18030_page_multiple(pe_test_config *tc)
{
    PQExpBuffer testname;
    size_t input_len = 0x20000;  // 128 KiB to hit page boundaries
    char *input;

    // Create large buffer ending with invalid GB18030 byte
    input = pg_malloc(input_len);
    memset(input, '-', input_len - 1);
    input[input_len - 1] = 0xfe;  // Invalid GB18030 byte

    // Create test description
    testname = createPQExpBuffer();
    appendPQExpBuffer(testname, ">repeat(%c, %zu)", input[0], input_len - 1);
    escapify(testname, input + input_len - 1, 1);
    appendPQExpBuffer(testname, "< - GB18030 - PQescapeLiteral");

    // Test memory boundary safety
    PQsetClientEncoding(tc->conn, "GB18030");
    report_result(tc, PQescapeLiteral(tc->conn, input, input_len) == NULL,
                  testname->data, "",
                  "input validity vs escape success", "ok");

    // Cleanup
    destroyPQExpBuffer(testname);
    pg_free(input);
}
```