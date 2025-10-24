# CHECK_SIZE_CODE

## Location
[src/tools/pg_bsd_indent/indent_globs.h:60-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/indent_globs.h#L60-L70)

## Overview
A macro that ensures sufficient buffer space for code formatting operations in the pg_bsd_indent tool, automatically reallocating memory when needed.

## Definition

```c
#define CHECK_SIZE_CODE(desired_size) \
	if (e_code + (desired_size) >= l_code) { \
	    int nsize = l_code-s_code + 400 + desired_size; \
	    int code_len = e_code-s_code; \
	    codebuf = (char *) realloc(codebuf, nsize); \
	    if (codebuf == NULL) \
		err(1, NULL); \
	    e_code = codebuf + code_len + 1; \
	    l_code = codebuf + nsize - 5; \
	    s_code = codebuf + 1; \
	}
```
## Detailed Description
This macro is a critical memory management utility used throughout the pg_bsd_indent tool to prevent buffer overflows when formatting C code. It checks if the current position in the code buffer () plus the desired additional space would exceed the buffer limit (). If so, it automatically reallocates the  with additional space (400 bytes plus the requested size) and updates all related pointers accordingly.

The macro implements a dynamic buffer expansion strategy that ensures the code formatting process never runs out of space. It includes error handling that terminates the program if memory allocation fails, which is appropriate for a command-line tool where recovery from memory allocation failure would be complex.

## Parameters / Member Variables
- `desired_size`: The number of additional bytes needed in the code buffer
## Dependencies
- Functions called/Symbols referenced:
  - realloc (standard C library function)
  - [err](../e/err.md) (error handling function)
- Global variables used:
  - : Current end position in the code buffer
  - : Limit/end of the code buffer
  - : Start of the code buffer
  - : The main code buffer pointer
- Called from (representative examples):
  - [main](../m/main.md) function (multiple times at lines 495, 514, 637, 648, 990, 1004)
  - [indent_declaration](../i/indent_declaration.md) function (lines 1258, 1266)

## Notes and Other Information
- This macro is essential for the dynamic memory management strategy of pg_bsd_indent
- The 400-byte safety margin provides reasonable buffer space to reduce frequent reallocations
- The macro updates three key pointers after reallocation: , , and
- Error handling is immediate and fatal - the program exits if memory allocation fails
- Used extensively throughout the indentation process to ensure buffer capacity before writing formatted code

## Simplified Source

```c
#define CHECK_SIZE_CODE(desired_size) \
    if (e_code + (desired_size) >= l_code) { \
        // Calculate new buffer size with safety margin
        int nsize = l_code - s_code + 400 + desired_size; \
        int code_len = e_code - s_code; \

        // Reallocate buffer
        codebuf = (char *) realloc(codebuf, nsize); \
        if (codebuf == NULL) \
            err(1, NULL); \

        // Update all buffer pointers
        e_code = codebuf + code_len + 1; \
        l_code = codebuf + nsize - 5; \
        s_code = codebuf + 1; \
    }
```