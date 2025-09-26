# xml_repalloc

## Location
[src/backend/utils/adt/xml.c:1969-1975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1969-L1975)

## Overview
A memory reallocation wrapper function that resizes previously allocated memory blocks for use by libxml operations through PostgreSQL's memory management system.

## Definition
```c
static void *xml_repalloc(void *ptr, size_t size)
```

## Detailed Description
This function serves as a custom memory reallocation callback for the libxml library. It wraps PostgreSQL's repalloc function to resize memory blocks that were previously allocated within the LibxmlContext. This ensures consistent memory management behavior where libxml can resize memory blocks while staying within PostgreSQL's memory management framework.

## Parameters / Member Variables
- `ptr`: Pointer to the previously allocated memory block to resize
- `size`: New size in bytes for the memory block

## Dependencies
- Functions called/Symbols referenced:
  - [repalloc](../r/repalloc.md)
- Called from:
  - [PgXmlErrorContext](../P/PgXmlErrorContext.md)
  - [xml_memory_init](xml_memory_init.md) (as callback registration)
  - Indirectly called by libxml through callback mechanism

## Notes and Other Information
- This is a static function used internally within the xml.c module
- Serves as a callback function registered with libxml via xmlMemSetup
- Part of PostgreSQL's integration with libxml for consistent memory management
- The function relies on PostgreSQL's repalloc which can handle memory allocated in any memory context
- If ptr is NULL, this behaves like a regular allocation; if size is 0, this behaves like a free operation