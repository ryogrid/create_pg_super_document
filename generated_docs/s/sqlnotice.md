# sqlnotice

## Location
[src/interfaces/ecpg/test/expected/preproc-init.c:117-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-init.c#L117-L125)

## Overview
A static callback function used in ECPG test programs to handle SQL notice messages during database operations.

## Definition

```c
struct sa x = { 14 },*y = &x;
```
## Detailed Description
The  function is a simple callback handler designed for use in ECPG (Embedded SQL in C) test programs. It receives SQL notice messages and transaction status information, providing a standardized way to display notice information during test execution. The function performs basic null-checking on the notice parameter and outputs formatted information to stdout for debugging and testing purposes.

## Parameters / Member Variables
- : A constant character pointer containing the notice message text. If NULL, the function substitutes "-empty-" as a default value
- : A short integer representing the transaction status or context information

## Dependencies
- Functions called/Symbols referenced:
  - printf (standard C library function)
- Called from (representative examples):
  - [main](../m/main.md) (in the same test file)

## Notes and Other Information
- This is a test-specific function located in the ECPG preprocessor test suite
- The function is marked as static, limiting its scope to the current compilation unit
- It serves as a demonstration of how notice callbacks can be implemented in ECPG applications
- The output format is designed for test verification and debugging purposes
- File location: src/interfaces/ecpg/test/expected/preproc-init.c:117-125

## Simplified Source

```c
static void sqlnotice(const char *notice, short trans) {
    // Handle null notice parameter with default value
    if (!notice)
        notice = "-empty-";

    // Print notice and transaction info for ECPG test
    printf("in sqlnotice (%s, %d)\n", notice, trans);
}
```