# array_desc

## Location
[src/backend/access/rmgrdesc/rmgrdesc_utils.c:24-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/rmgrdesc_utils.c#L24-L43)

## Overview
A utility function that formats and prints array contents into a StringInfo buffer for WAL record description purposes.

## Definition

```c
void
array_desc(StringInfo buf, void *array, size_t elem_size, int count,
		   void (*elem_desc) (StringInfo buf, void *elem, void *data),
		   void *data)
```
## Detailed Description
The  function is a helper utility designed to format array contents for WAL (Write-Ahead Logging) record descriptions in a standardized format. It takes a generic array and uses a provided callback function to describe each element, outputting the results in a comma-separated list enclosed in square brackets. This function is part of the PostgreSQL resource manager description utilities that help format WAL records for debugging and logging purposes.

The function handles empty arrays by outputting "[]" and non-empty arrays by iterating through each element, calling the provided  callback function for each one, and separating elements with commas.

## Parameters / Member Variables
- : StringInfo buffer where the formatted array description will be appended
- : Pointer to the array data to be described
- : Size in bytes of each array element
- : Number of elements in the array
- : Callback function pointer that formats individual array elements
- : Additional data passed to the element description callback function

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfoString](appendStringInfoString.md)
  - [appendStringInfoChar](appendStringInfoChar.md)
- Called from (representative examples):
  - [plan_elem_desc](../p/plan_elem_desc.md)
  - [heap_desc](../h/heap_desc.md)
  - [heap2_desc](../h/heap2_desc.md)
  - [delvacuum_desc](../d/delvacuum_desc.md)

## Notes and Other Information
- This function is part of the WAL record description infrastructure used for debugging and monitoring
- The function uses a generic approach with callbacks to handle different element types
- Output format follows PostgreSQL's standard array representation with square brackets
- Located in the rmgrdesc_utils.c file which contains utilities for resource manager descriptions
- The function is declared in rmgrdesc_utils.h for use across different resource manager description modules

## Simplified Source

```c
void array_desc(StringInfo buf, void *array, size_t elem_size, int count,
               void (*elem_desc) (StringInfo buf, void *elem, void *data),
               void *data) {
    // Handle empty arrays
    if (count == 0) {
        appendStringInfoString(buf, " []");
        return;
    }

    // Start array output
    appendStringInfoString(buf, " [");

    // Process each element using callback function
    for (int i = 0; i < count; i++) {
        elem_desc(buf, (char *) array + elem_size * i, data);
        if (i < count - 1)
            appendStringInfoString(buf, ", ");
    }

    // Close array output
    appendStringInfoChar(buf, ']');
}
```