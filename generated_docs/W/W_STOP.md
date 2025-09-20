# W_STOP

## Location
[src/interfaces/ecpg/preproc/type.h:84-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.h#L84-L86)

## Overview
W_STOP is an enumeration value in the WHEN_TYPE enum that represents a stop action in ECPG (Embedded SQL in C) whenever statements, causing program termination.

## Definition

```c
struct when
{
	enum WHEN_TYPE code;
	char	   *command;
	char	   *str;
};
```
## Detailed Description
W_STOP is one of the enumeration values in the WHEN_TYPE enum used by the ECPG preprocessor to handle different types of actions in SQL exception handling statements. When W_STOP is encountered, it generates C code that calls "exit(1)" to terminate the program with an error status. This is typically used in WHENEVER statements to stop program execution when a specific SQL condition (like SQLERROR or SQLWARNING) occurs.

## Parameters / Member Variables

W_STOP is an enumeration constant, not a struct, so it has no member variables. It represents a stop action value in the WHEN_TYPE enum.

## Dependencies
- Functions called/Symbols referenced:
  - N/A (enumeration constant)
- Called from (representative examples):
  - [print_action](../p/print_action.md) (in src/interfaces/ecpg/preproc/output.c:50)

## Notes and Other Information
- Defined in src/interfaces/ecpg/preproc/type.h:84
- Part of the ECPG preprocessor's exception handling mechanism
- When processed by print_action(), generates "exit(1);" in the output C code
- Used in conjunction with WHENEVER statements in embedded SQL programs
- Provides a way to immediately terminate program execution upon SQL errors or warnings