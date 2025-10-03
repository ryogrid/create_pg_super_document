# test_strlower

## Location
[src/common/unicode/case_test.c:89-169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode/case_test.c#L89-L169)

## Overview
A comprehensive test function that validates the  function's behavior across different string termination scenarios and memory management edge cases.

## Definition

```c
static void
test_strlower(const char *test_string, const char *expected)
```
## Detailed Description
This function performs rigorous testing of PostgreSQL's  function by testing four different combinations of source and destination string termination states:

1. **Test 1**: Neither source nor destination are NUL-terminated
2. **Test 2**: Destination is NUL-terminated, source is not
3. **Test 3**: Source is NUL-terminated, destination is not  
4. **Test 4**: Both source and destination are NUL-terminated

For each test scenario, the function verifies both the returned length value and the actual converted content. It uses careful memory management with malloc/free and intentionally fills destination buffers with 0x7F to detect buffer overruns or incomplete writes. The function terminates immediately with exit(1) if any test fails, providing detailed diagnostic output.

## Parameters / Member Variables
- `*test_string`: The input string to be converted to lowercase
- `*expected`: The expected result string after lowercase conversion
## Dependencies
- Functions called/Symbols referenced:
  - [unicode_strlower](../u/unicode_strlower.md)
  - strlen
  - malloc
  - strdup
  - memcpy
  - memset
  - memcmp
  - strcmp
  - printf
  - exit
  - free
- Called from (representative examples):
  - [test_convert_case](test_convert_case.md)

## Notes and Other Information
- This is a static function, only accessible within the case_test.c compilation unit
- Performs comprehensive boundary testing by testing all four combinations of NUL-termination states
- Uses defensive programming by filling buffers with 0x7F to detect write errors
- Memory allocation/deallocation is carefully managed with malloc/free pairs
- The function validates both functional correctness (proper case conversion) and API contract compliance (correct length reporting)
- Critical for ensuring  handles various real-world string handling scenarios correctly
- Part of PostgreSQL's Unicode case conversion testing suite