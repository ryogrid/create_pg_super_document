# add_typedefs_from_file

## Location
[src/tools/pg_bsd_indent/args.c:335-350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/args.c#L335-L350)

## Overview
Reads a file containing type names line-by-line and adds each type name to the global typename list used by pg_bsd_indent for proper code formatting.

## Definition
```c
void add_typedefs_from_file(const char *str)
```

## Detailed Description
This function is part of pg_bsd_indent (PostgreSQL's BSD-style code indenter) and serves to populate the indenter's type name registry from an external file. The function opens a specified file, reads each line as a type name (after trimming whitespace), and adds it to the internal typename database using `add_typename()`. This allows the indenter to recognize user-defined types and format code involving these types correctly.

The function handles file I/O errors by printing an error message and terminating the program. Each line in the file is processed by removing trailing whitespace before being passed to `add_typename()` for registration in the sorted typename array.

This functionality is typically triggered by the command-line option `-U filename` where the filename contains a list of type names, one per line.

## Parameters / Member Variables
- `str`: Path to the file containing type names, one per line. The file should contain type names without any additional formatting or decorations.

## Dependencies
- Functions called/Symbols referenced:
  - `fopen`: Opens the specified file for reading
  - `fgets`: Reads lines from the file
  - `strcspn`: Used to find and remove trailing whitespace  
  - `add_typename`: Registers each type name in the typename database
  - `fclose`: Closes the file after processing
  - `fprintf`: Prints error message if file cannot be opened
  - `exit`: Terminates program on file open error

- Called from:
  - `set_option`: Called when processing the `-U` command-line option (KEY_FILE case)

## Notes and Other Information
- The function uses a fixed buffer size (BUFSIZ) to read lines from the file
- If the file cannot be opened, the program terminates with exit code 1
- Trailing whitespace (spaces, tabs, newlines, carriage returns) is automatically stripped from each line
- Empty lines or lines containing only whitespace will result in empty strings being passed to `add_typename()`, which may be handled by that function
- The function is specifically designed for pg_bsd_indent's command-line interface and integrates with the broader argument processing system
- Related to the `-T` option which adds individual type names, while `-U` processes entire files of type names