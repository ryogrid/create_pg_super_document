# hashline_number

## Location
[src/interfaces/ecpg/preproc/output.c:94-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/output.c#L94-L135)

## Overview
Generates a C preprocessor line directive string that maps generated code back to the original source file and line number.

## Definition
```c
char *hashline_number(void)
```

## Detailed Description
This function creates a properly formatted C preprocessor line directive (`#line`) that maintains the correspondence between generated C code and the original ECPG source file. It constructs a string containing the current line number and filename, with proper escaping of backslashes and quotes in the filename. The function includes debug mode handling and returns either a formatted line directive or an empty string based on the current state. The returned string must be freed by the caller.

## Parameters / Member Variables
- Uses global variables:
  - `input_filename`: Current input filename
  - `base_yylineno`: Current line number in the input file
  - `base_yydebug`: Debug mode flag (when YYDEBUG is defined)

## Dependencies
- Functions called/Symbols referenced:
  - [mm_alloc](../m/mm_alloc.md) (memory allocation)
  - sprintf
  - strlen
  - strcat
  - EMPTY (constant for empty string)
  - CHAR_BIT (system constant)
- Called from:
  - [output_line_number](../o/output_line_number.md) (src/interfaces/ecpg/preproc/output.c:12)

## Notes and Other Information
- Returns dynamically allocated memory that must be freed by the caller
- Handles special characters in filenames by escaping backslashes and quotes
- Skips line directive generation in debug mode (when YYDEBUG is enabled and base_yydebug is true)
- Uses careful memory allocation calculation to handle worst-case filename escaping
- Essential for maintaining source-to-generated code mapping for debugging and error reporting
- Part of the ECPG preprocessor's line tracking mechanism

## Simplified Source

```c
char *hashline_number(void) {
    // Skip line numbers in debug mode or when no input filename
    if (input_filename
#ifdef YYDEBUG
        && !base_yydebug
#endif
        ) {
        // Allocate memory for line directive with escaping
        char *line = mm_alloc(strlen("\n#line %d \"%s\"\n") + sizeof(int) * CHAR_BIT * 10 / 3 + strlen(input_filename) * 2);
        char *src, *dest;

        // Start building the line directive
        sprintf(line, "\n#line %d \"", base_yylineno);

        // Escape filename characters (backslash and quote)
        src = input_filename;
        dest = line + strlen(line);
        while (*src) {
            if (*src == '\\' || *src == '"')
                *dest++ = '\\';
            *dest++ = *src++;
        }
        *dest = '\0';
        strcat(dest, "\"\n");

        return line;
    }

    return EMPTY;
}
```