# ParseVariableNum

## Location
[src/bin/psql/variables.c:156-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/variables.c#L156-L185)

## Overview
Parses a string value as an integer and stores the result, with comprehensive validation to ensure the value fits within integer bounds.

## Definition

```c
struct _variable *ptr;
```
## Detailed Description
This function attempts to parse a string as an integer value using the standard C library strtol function with automatic base detection (base 0). It performs thorough validation to ensure the string represents a valid integer that fits within the range of the int data type. The function checks for conversion errors, ensures the entire string was consumed during parsing, verifies that at least one character was processed, and confirms the resulting long value can be safely cast to an int.

The function treats NULL input as an empty string, which results in a parsing error. When parsing fails, the original value in the result parameter is preserved, maintaining the principle of not clobbering the output on failure. Error reporting is optional and controlled by the name parameter.

## Parameters / Member Variables
- : The string value to parse as an integer. NULL is treated as an empty string
- : The name of the variable being assigned (used for error reporting). Pass NULL to suppress error messages  
- : Pointer to int where the parsed result will be stored. Only modified on successful parsing

## Dependencies
- Functions called/Symbols referenced:
  - strtol (standard C library function for string to long conversion)
  - pg_log_error (PostgreSQL error logging function)
- Called from (representative examples):
  - fmt (formatting command)
  - [fetch_count_hook](../f/fetch_count_hook.md)
  - [histsize_hook](../h/histsize_hook.md)
  - [ignoreeof_substitute_hook](../i/ignoreeof_substitute_hook.md)
  - [ignoreeof_hook](../i/ignoreeof_hook.md)

## Notes and Other Information
- Uses strtol with base 0 for automatic base detection (supports decimal, octal with 0 prefix, hex with 0x prefix)
- Validates that the entire string was consumed (no trailing characters)
- Ensures the parsed long value fits within int range before assignment
- Returns true for successful parsing, false for invalid input
- Preserves original *result value when parsing fails
- Used for parsing various numeric configuration variables in psql
- Comprehensive error checking prevents integer overflow and invalid input acceptance

## Simplified Source

```c
bool ParseVariableNum(const char *value, const char *name, int *result) {
    char *end;
    long numval;

    // Treat NULL as empty string
    if (value == NULL)
        value = "";

    // Parse string to long integer
    errno = 0;
    numval = strtol(value, &end, 0);

    // Check for successful parsing: no errors, entire string consumed,
    // at least one digit processed, and value fits in int range
    if (errno == 0 && *end == '\0' && end != value && numval == (int) numval) {
        *result = (int) numval;
        return true;
    } else {
        // Log error if name provided, don't modify result
        if (name)
            pg_log_error("invalid value \"%s\" for \"%s\": integer expected",
                         value, name);
        return false;
    }
}
```