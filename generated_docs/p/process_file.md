# process_file

## Location
[src/bin/psql/command.c:4380-4446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L4380-L4446)

## Overview
A utility function in pgbench that reads a file containing SQL script content and adds it to the list of scripts to be executed during benchmarking.

## Definition

```c
int
process_file(char *filename, bool use_relative_path)
```
## Detailed Description
The  function is responsible for reading script files in pgbench, PostgreSQL's built-in benchmarking tool. It handles file I/O operations to read SQL script content from either a specified file or stdin (when filename is "-"), then parses the content and adds it to the script collection with the specified weight. The function includes proper error handling for file operations and memory management for the file contents.

## Parameters / Member Variables
- `*filename`: Path to the script file to be processed. Special value "-" indicates reading from stdin
- `use_relative_path`: Numeric weight value assigned to this script, affecting its selection probability during benchmark execution
## Dependencies
- Functions called/Symbols referenced:
  - fopen (standard library function for file opening)
  - [read_file_contents](../r/read_file_contents.md) (pgbench utility to read entire file into memory)
  - [ParseScript](../P/ParseScript.md) (pgbench function to parse and register the script)
- Called from (representative examples):
  - [main](../m/main.md) (in pgbench.c for processing command-line specified script files)
  - [exec_command_include](../e/exec_command_include.md) (in psql for including files)

## Notes and Other Information
- The function is declared as static, limiting its scope to the pgbench.c compilation unit
- Includes robust error handling with pg_fatal calls for file operation failures
- Automatically handles both regular files and stdin input transparently
- Memory management is properly handled with free() call after script parsing
- The filename parameter storage must persist as noted in the comment, suggesting the filename string is retained by ParseScript

## Simplified Source

```c
static void
process_file(const char *filename, int weight)
{
    FILE *fd;
    char *buf;

    // Open file or use stdin
    if (strcmp(filename, "-") == 0)
        fd = stdin;
    else if ((fd = fopen(filename, "r")) == NULL)
        pg_fatal("could not open file \"%s\": %m", filename);

    // Read entire file contents
    buf = read_file_contents(fd);

    // Check for read errors
    if (ferror(fd))
        pg_fatal("could not read file \"%s\": %m", filename);

    // Close file if not stdin
    if (fd != stdin)
        fclose(fd);

    // Parse and register the script
    ParseScript(buf, filename, weight);

    // Clean up
    free(buf);
}
```