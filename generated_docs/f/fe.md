# fe

## Location
[src/interfaces/ecpg/test/expected/preproc-init.c:111-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-init.c#L111-L116)

## Overview
A static test function used in ECPG (Embedded SQL in C) test cases that demonstrates enum parameter handling and type casting operations.

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
The `fe` function is a test function that takes an enumeration parameter of type `enum e`, prints its integer value to stdout, and returns the enum value cast to an integer. This function is part of the ECPG test suite and is used to verify that enum parameter passing and type casting work correctly in the ECPG preprocessor and runtime environment.

## Parameters / Member Variables
- `x`: An enumeration parameter of type `enum e` that is converted to integer and returned

## Dependencies
- Functions called/Symbols referenced:
  - printf (standard C library function)
  - enum e (enumeration type defined in the same file)
- Called from (representative examples):
  - [main](../m/main.md) (in src/interfaces/ecpg/test/expected/preproc-init.c:248)

## Notes and Other Information
- This is a static function with internal linkage, only visible within its compilation unit
- Demonstrates enum parameter passing in ECPG test scenarios
- Performs explicit type casting from enum to int
- Part of the ECPG test infrastructure for validating embedded SQL functionality
- Uses explicit casting `(int) x` to convert enum values to integers for both printing and returning
- The function validates that enum types can be properly processed through the ECPG preprocessor