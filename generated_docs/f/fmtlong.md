# fmtlong

## Location
[src/interfaces/ecpg/test/expected/compat_informix-rfmtlong.c:27-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/compat_informix-rfmtlong.c#L27-L43)

## Overview
A static helper function that wraps the rfmtlong() function to format long integers according to a specified format string and handle return codes.

## Definition

```c
static void
fmtlong(long lng, const char *fmt)
```
## Detailed Description
The fmtlong function serves as a testing wrapper around the rfmtlong() function in the ECPG Informix compatibility library. It formats a long integer value according to a provided format string, prints the result or error status, and maintains a static counter for successful conversions. This function is primarily used in test code to validate the behavior of the rfmtlong() formatting function.

The function calls rfmtlong() to perform the actual formatting, then examines the return code. If the formatting succeeds (return code 0), it prints the formatted result along with a sequence number. If formatting fails, it delegates error handling to the check_return() function.

## Parameters / Member Variables
- `lng`: The long integer value to be formatted
- `*fmt`: A pointer to a null-terminated string containing the format specification
## Dependencies
- Functions called/Symbols referenced:
  - [rfmtlong](../r/rfmtlong.md)
  - printf
  - [check_return](../c/check_return.md)
- Called from (representative examples):
  - [main](../m/main.md) (multiple times in test scenarios)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within its compilation unit
- Uses a static integer counter 'i' to track the number of successful formatting operations
- The function assumes a buffer size of 30 characters for the formatted output
- This function is part of the ECPG (Embedded SQL in C for PostgreSQL) test suite for Informix compatibility
- Located in a test expected output file, indicating it's used for regression testing

## Simplified Source

```c
static void
fmtlong(long lng, const char *fmt)
{
    static int i;        // Counter for successful formatting operations
    int r;              // Return code from rfmtlong
    char buf[30];       // Buffer for formatted output

    // Call the actual formatting function
    r = rfmtlong(lng, fmt, buf);
    printf("r: %d ", r);

    // Check result and print output or error
    if (r == 0)
    {
        // Success: print formatted result with sequence number
        printf("%d: %s (fmt was: %s)\n", i++, buf, fmt);
    }
    else
    {
        // Error: delegate to error handling function
        check_return(r);
    }
}
```