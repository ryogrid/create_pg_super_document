# fc

## Location
[src/interfaces/ecpg/test/expected/preproc-init.c:99-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-init.c#L99-L104)

## Overview
A static test function used in ECPG (Embedded SQL in C) test cases that demonstrates string parameter handling and character extraction.

## Definition

```c
enum e x)
{
	printf("in fe (%d)\n", (int) x);
	return (int)x;
}

static void sqlnotice(const char *notice, short trans)
{
	if (!notice)
		notice = "-empty-";
	printf("in sqlnotice (%s, %d)\n", notice, trans);
}



#define YES 1

#ifdef _cplusplus
namespace N
{
	static const int i=2;
};
```
## Detailed Description
The `fc` function is a test function that takes a constant string pointer parameter, prints it to stdout, and returns the first character of the string as an integer. This function is part of the ECPG test suite and is used to verify that string parameter passing and character operations work correctly in the ECPG preprocessor and runtime environment.

## Parameters / Member Variables
- `x`: A constant character pointer (string) that is printed and whose first character is returned

## Dependencies
- Functions called/Symbols referenced:
  - printf (standard C library function)
- Called from (representative examples):
  - [main](../m/main.md) (in src/interfaces/ecpg/test/expected/preproc-init.c:230)

## Notes and Other Information
- This is a static function with internal linkage, only visible within its compilation unit
- Demonstrates string parameter passing in ECPG test scenarios
- Returns the ASCII value of the first character in the input string
- Part of the ECPG test infrastructure for validating embedded SQL functionality
- The function name 'fc' appears to be referenced extensively in backend parser code, though this may be coincidental symbol name matching with different contexts
- Uses const-correctness for the string parameter, indicating it doesn't modify the input string