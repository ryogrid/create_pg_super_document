# add_reloption_kind

## Location
[src/backend/access/common/reloptions.c:683-699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L683-L699)

## Overview
The add_reloption_kind function creates a new relopt_kind value for use in custom relation options by user-defined access methods (AMs).

## Definition

```c
enum's behavior is portable */
	if (last_assigned_kind >= RELOPT_KIND_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("user-defined relation parameter types limit exceeded")));
```
## Detailed Description
This function generates unique relation option kind identifiers for custom access methods that need to define their own relation options. It implements a simple bit-shifting allocation scheme to ensure each custom AM gets a unique identifier. The function tracks the last assigned kind using a static variable and shifts it left by one bit for each new allocation, effectively creating powers-of-2 identifiers.

The function includes safety checks to prevent exceeding the maximum number of relation option kinds (RELOPT_KIND_MAX), ensuring that the enum behavior remains portable across different platforms.

## Parameters / Member Variables
This function takes no parameters and returns a relopt_kind value.

## Return Value
- **relopt_kind**: A unique identifier for the new relation option kind, represented as a power-of-2 value

## Dependencies
- Functions called/Symbols referenced:
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Constants used:
  - RELOPT_KIND_MAX
  - ERROR
  - ERRCODE_PROGRAM_LIMIT_EXCEEDED
- Global variables accessed:
  - last_assigned_kind (static variable)
- Called from:
  - GET_STRING_RELOPTION (macro)
  - [create_reloptions_table](../c/create_reloptions_table.md)

## Notes and Other Information
- The function uses bit-shifting (<<= 1) to generate unique power-of-2 identifiers
- Each call doubles the value of last_assigned_kind, ensuring uniqueness
- The maximum limit check prevents overflow and maintains enum portability
- This function is typically called during extension or custom access method initialization
- The returned relopt_kind can be used with add_reloption to register custom options
- Error handling ensures graceful failure when the system limit is exceeded

## Simplified Source

```c
relopt_kind add_reloption_kind(void) {
    // Check if we've exceeded the maximum allowed kinds
    if (last_assigned_kind >= RELOPT_KIND_MAX) {
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("user-defined relation parameter types limit exceeded")));
    }

    // Shift to next power-of-2 value for unique identifier
    last_assigned_kind <<= 1;
    return (relopt_kind) last_assigned_kind;
}
```