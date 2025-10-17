# readfile

## Location
[src/bin/pg_ctl/pg_ctl.c:313-408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L313-L408)

## Overview
Reads all lines from a text file and returns them as a dynamically allocated array of strings, with automatic memory management for growing arrays.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
The  function is a utility function in initdb that reads an entire text file and returns its contents as an array of strings. Each line in the file becomes a separate string in the result array. The function handles dynamic memory allocation, automatically growing the result array as needed while reading the file.

The function uses PostgreSQL's string handling utilities (, ) to efficiently read lines of arbitrary length. It starts with an initial capacity of 1024 lines and doubles the capacity when more space is needed. Each line is duplicated into individually allocated strings using . The result array is NULL-terminated to make it easy to iterate over.

Memory management follows PostgreSQL patterns using , , and  functions. If the file cannot be opened, the function calls  to report the error and exit.

## Parameters / Member Variables
- : The file system path to the text file to be read

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library)
  -  (PostgreSQL error handling)
  -  (PostgreSQL string utilities)
  -  (PostgreSQL memory allocation)
  -  (PostgreSQL line reading utility)
  -  (PostgreSQL memory reallocation) 
  -  (PostgreSQL string duplication)
  -  (PostgreSQL memory deallocation)
  -  (standard C library)
- Called from (representative examples):
  -  (initdb.c:1277, 1444, 1511)
  -  (initdb.c:1539)
  -  (initdb.c:1715)
  -  (pg_ctl.c:804)
  -  (pg_ctl.c:1367)

## Notes and Other Information
- Returns a malloc'd array of individually malloc'd strings
- [Result](../R/Result.md) array is NULL-terminated for easy iteration
- Automatically handles files of arbitrary size through dynamic reallocation
- Initial capacity is 1024 lines, doubles when needed
- Uses PostgreSQL memory management functions throughout
- Static function, only available within initdb.c
- Companion function to  for memory cleanup
- Essential for reading configuration templates and SQL scripts during database initialization
- Handles empty files correctly (returns array with single NULL element)

## Simplified Source

```c
static char **readfile(const char *path) {
    char **result;
    FILE *infile;
    StringInfoData line;
    int maxlines;
    int n;

    // Open file for reading
    if ((infile = fopen(path, "r")) == NULL)
        pg_fatal("could not open file \"%s\" for reading: %m", path);

    initStringInfo(&line);

    // Start with capacity for 1024 lines
    maxlines = 1024;
    result = (char **) pg_malloc(maxlines * sizeof(char *));

    // Read lines from file
    n = 0;
    while (pg_get_line_buf(infile, &line)) {
        // Expand array if needed (leave room for NULL terminator)
        if (n >= maxlines - 1) {
            maxlines *= 2;
            result = (char **) pg_realloc(result, maxlines * sizeof(char *));
        }

        // Copy line to result array
        result[n++] = pg_strdup(line.data);
    }

    // NULL-terminate the array
    result[n] = NULL;

    // Cleanup
    pfree(line.data);
    fclose(infile);

    return result;
}
```