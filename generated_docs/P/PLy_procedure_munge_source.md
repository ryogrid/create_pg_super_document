# PLy_procedure_munge_source

## Location
[src/pl/plpython/plpy_procedure.c:429-471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_procedure.c#L429-L471)

## Overview
PLy_procedure_munge_source is a static utility function that transforms raw PL/Python function source code into a properly formatted Python function definition with correct indentation.

## Definition
```c
static char *PLy_procedure_munge_source(const char *name, const char *src)
```

## Detailed Description
This function takes raw PL/Python procedure source code and transforms it into a valid Python function definition. It creates a new string buffer containing a Python function definition header ("def name():") followed by the original source code with proper indentation. The function handles different line ending conventions (\r\n, \n, \r) and ensures all lines of the original source are indented with a tab character to create syntactically correct Python code. The function also includes buffer overflow protection to prevent memory corruption.

## Parameters / Member Variables
- `name`: The name of the PL/Python function to be created
- `src`: The raw source code of the procedure that needs to be formatted

## Dependencies
- Functions called/Symbols referenced:
  - strlen (C standard library function)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)  
  - snprintf (C standard library function)
  - Assert (PostgreSQL assertion macro)
  - elog (PostgreSQL logging function)
- Called from (representative examples):
  - [PLy_procedure_compile](PLy_procedure_compile.md)

## Notes and Other Information
- This is a static function, only accessible within the plpy_procedure.c file
- Allocates buffer size of (strlen(src) * 2) + strlen(name) + 16 to accommodate formatting
- Handles cross-platform line ending compatibility (\r\n on Windows, \n on Unix)
- Adds proper Python indentation (tab characters) after each newline
- Includes buffer overflow detection as a safety measure
- Returns a palloc'd string that must be freed by the caller
- Essential for converting PostgreSQL function definitions into executable Python code

## Simplified Source

```c
static char *PLy_procedure_munge_source(const char *name, const char *src) {
    // Calculate buffer size for transformed source
    size_t buffer_len = (strlen(src) * 2) + strlen(name) + 16;
    char *result = palloc(buffer_len);

    // Create Python function definition header
    int header_len = snprintf(result, buffer_len, "def %s():\n\t", name);
    Assert(header_len >= 0 && header_len < buffer_len);

    // Transform source by adding proper indentation
    const char *src_ptr = src;
    char *dest_ptr = result + header_len;

    while (*src_ptr != '\0') {
        // Handle different line ending conventions
        if (*src_ptr == '\r' && *(src_ptr + 1) == '\n')
            src_ptr++;  // Skip \r in \r\n sequence

        if (*src_ptr == '\n' || *src_ptr == '\r') {
            // Add newline and tab indentation
            *dest_ptr++ = '\n';
            *dest_ptr++ = '\t';
            src_ptr++;
        } else {
            *dest_ptr++ = *src_ptr++;
        }
    }

    // Terminate with double newline
    *dest_ptr++ = '\n';
    *dest_ptr++ = '\n';
    *dest_ptr = '\0';

    // Safety check for buffer overflow
    if (dest_ptr > (result + buffer_len))
        elog(FATAL, "buffer overrun in PLy_procedure_munge_source");

    return result;
}
```