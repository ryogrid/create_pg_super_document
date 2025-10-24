# PrintString

## Location
[src/test/modules/test_resowner/test_resowner_basic.c:44-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_resowner/test_resowner_basic.c#L44-L51)

## Overview
PrintString is a static callback function used in PostgreSQL resource owner testing that formats a string resource into a printable representation for debugging and logging purposes.

## Definition
```c
static char *PrintString(Datum res)
```

## Detailed Description
This function serves as a resource print callback in the PostgreSQL resource owner testing framework. It takes a Datum containing a string pointer and formats it into a human-readable string representation enclosed in quotes with a "test string" prefix. The function uses psprintf to dynamically allocate and format the return string, which is useful for debugging and tracking resource states in the resource owner subsystem.

## Parameters / Member Variables
- `res`: A Datum containing a pointer to the string resource that needs to be formatted for display

## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md) (for formatted string allocation and formatting)
  - [DatumGetPointer](../D/DatumGetPointer.md) (to extract the pointer from the Datum)
- Called from (representative examples):
  - [test_resowner_priorities](../t/test_resowner_priorities.md) (used as callback in resource owner registration)

## Notes and Other Information
- This function is part of the test_resowner module, specifically designed for testing resource owner functionality
- Returns a dynamically allocated string that should be freed by the caller
- The formatted output helps identify and track string resources during testing and debugging
- Used in conjunction with resource owner registration to provide human-readable resource descriptions
- Follows the standard PostgreSQL resource print callback signature pattern

## Simplified Source

```c
static char *
PrintString(Datum res)
{
    // Convert Datum to string pointer and format with prefix
    return psprintf("test string \"%s\"", DatumGetPointer(res));
}
```