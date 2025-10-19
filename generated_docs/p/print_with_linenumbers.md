# print_with_linenumbers

## Location
[src/bin/psql/command.c:5914-5955](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5914-L5955)

## Overview
Outputs text with line numbers, specially handling function definitions by only numbering the function body lines while leaving the header unnumbered.

## Definition
```c
static void print_with_linenumbers(FILE *output, char *lines, bool is_func)
```

## Detailed Description
This function formats and prints text content with line numbers, implementing special logic for function definitions. When dealing with functions (is_func = true), it distinguishes between the function header (which remains unnumbered) and the function body (which gets numbered starting from 1). The function body is identified by lines beginning with "AS ", "BEGIN ", or "RETURN ", which are expected patterns from pg_get_functiondef() output.

The function processes the input text line by line, temporarily modifying the input string by null-terminating each line for printing, then restoring the newline to advance to the next line. Header lines are printed with 8-space indentation, while body lines are printed with a 7-character wide line number field.

## Parameters / Member Variables
- `output`: FILE pointer to the output stream where the formatted text will be written
- `lines`: Input text string that will be processed and printed with line numbers. The string is temporarily modified during processing but restored afterward
- `is_func`: Boolean flag indicating whether the text represents a function definition, which affects line numbering behavior

## Dependencies
- Functions called/Symbols referenced:
  - strncmp (standard C library)
  - strchr (standard C library)  
  - fprintf (standard C library)
- Called from (representative examples):
  - [exec_command_sf_sv](../e/exec_command_sf_sv.md) (in src/bin/psql/command.c:2580)

## Notes and Other Information
- The function modifies the input string temporarily by replacing newlines with null terminators, but restores them before advancing
- For functions, line numbering starts at 1 for the first line of the actual function body
- Header lines (before "AS ", "BEGIN ", or "RETURN ") are printed with 8-space padding instead of line numbers
- Function body lines are formatted with a 7-character left-aligned line number field
- The function assumes that pg_get_functiondef() output follows the expected format with recognizable function body start patterns
- Designed to work with psql's \sf and \sv commands for displaying function and view definitions with line numbers

## Simplified Source

```c
static void
print_with_linenumbers(FILE *output, char *lines, bool is_func)
{
    bool in_header = is_func;
    int lineno = 0;

    while (*lines != '\0') {
        char *eol;

        // Check if we've reached the function body
        if (in_header &&
            (strncmp(lines, "AS ", 3) == 0 ||
             strncmp(lines, "BEGIN ", 6) == 0 ||
             strncmp(lines, "RETURN ", 7) == 0))
            in_header = false;

        // Increment line number only for function body
        if (!in_header)
            lineno++;

        // Find and temporarily null-terminate current line
        eol = strchr(lines, '\n');
        if (eol != NULL)
            *eol = '\0';

        // Print line with appropriate formatting
        if (in_header)
            fprintf(output, "        %s\n", lines);  // Header: 8 spaces
        else
            fprintf(output, "%-7d %s\n", lineno, lines);  // Body: line number

        // Advance to next line
        if (eol == NULL)
            break;
        lines = ++eol;
    }
}
```