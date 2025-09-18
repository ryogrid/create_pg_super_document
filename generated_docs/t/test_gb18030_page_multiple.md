# test_gb18030_page_multiple

## Location
src/test/modules/test_escape/test_escape.c: 180 - 212

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
  - pe_test_config (struct type)
  - pg_malloc (PostgreSQL memory allocation)
  - memset (standard library function)
  - createPQExpBuffer (libpq buffer creation)
  - appendPQExpBuffer (libpq buffer append)
  - escapify (local utility function for readable output)
  - PQsetClientEncoding (libpq encoding setting)
  - PQescapeLiteral (libpq escape function being tested)
  - report_result (local test reporting function)
  - destroyPQExpBuffer (libpq buffer cleanup)
  - pg_free (PostgreSQL memory deallocation)
- Called from (representative examples):
  - main

## Notes and Other Information
- This is a static function, accessible only within the test_escape.c file
- Designed to run early in the test suite when freelists are minimal to maximize the chance of detecting boundary issues
- Uses a large buffer size (0x20000 = 128 KiB) to increase likelihood of hitting page boundaries
- Tests specifically for GB18030 encoding, which has complex multi-byte character sequences
- The test expects PQescapeLiteral to return NULL for invalid input, indicating graceful failure
- Part of PostgreSQL's memory safety testing infrastructure
- Comments indicate that smaller buffer sizes (4096 bytes) didn't trigger the issue, while 8192 bytes did on the tested system