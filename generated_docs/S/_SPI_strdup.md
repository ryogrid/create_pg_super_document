# _SPI_strdup

## Location
[src/backend/utils/adt/xml.c:2729-2785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2729-L2785)

## Overview
A static utility function that duplicates a string using SPI memory allocation, providing a SPI-compatible alternative to the standard  function.

## Definition

```c
structure (the "schema") to XML
 * Schema.  And there are functions that do both at the same time.
 *
 * Then you can map a database, a schema, or a table, each in both
 * ways.  This breaks down recursively: Mapping a database invokes
 * mapping schemas, which invokes mapping tables, which invokes
 * mapping rows, which invokes mapping columns, although you can't
 * call the last two from the outside.  Because of this, there are a
 * number of xyz_internal() functions which are to be called both from
 * the function manager wrapper and from some upper layer in a
 * recursive call.
 *
 * See the documentation about what the common function arguments
 * nulls, tableforest, and targetns mean.
 *
 * Some style guidelines for XML output: Use double quotes for quoting
 * XML attributes.  Indent XML elements by two spaces, but remember
 * that a lot of code is called recursively at different levels, so
 * it's better not to indent rather than create output that indents
 * and outdents weirdly.  Add newlines to make the output look nice.
 */


/*
 * Visibility of objects for XML mappings;
```
## Detailed Description
This function provides string duplication functionality specifically designed to work within the SPI (Server Programming Interface) memory context. It allocates memory using  instead of the standard memory allocator, ensuring that the duplicated string is allocated in the appropriate SPI memory context and will be properly managed by SPI's memory management system.

The function calculates the length of the input string (including the null terminator), allocates the appropriate amount of memory in the SPI context, and copies the entire string including the null terminator to the new memory location.

This is particularly important for functions that need to return string values that will persist beyond the current memory context or need to be managed by SPI's cleanup mechanisms.

## Parameters
- : The null-terminated input string to be duplicated

## Dependencies
- Functions called/Symbols referenced:
  - : Calculate the length of the input string (standard C library)
  - : Allocate memory in the SPI memory context
  - : Copy the string data to the newly allocated memory (standard C library)

- Called from (representative examples):
  - : XML schema generation from queries
  - : XML schema generation from cursors  
  - : Combined XML and schema generation

## Notes and Other Information
- This is a static function, only accessible within the same source file ()
- Uses SPI memory allocation to ensure proper memory management within SPI contexts
- Allocates  bytes to include space for the null terminator
- Returns a newly allocated string that is managed by SPI's memory context
- The copied string includes the null terminator, making it a proper C string
- Memory allocated by this function will be automatically freed when the SPI context is cleaned up
- More reliable than standard  in SPI contexts where memory management is critical