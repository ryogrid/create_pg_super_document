# xml_palloc

## Location
src/backend/utils/adt/xml.c: 1962 - 1968

## Overview
A memory allocation wrapper function that allocates memory within the special LibxmlContext memory context for use by libxml operations.

## Definition
```c
static void *xml_palloc(size_t size)
```

## Detailed Description
This function serves as a custom memory allocation callback for the libxml library. It wraps PostgreSQL's MemoryContextAlloc function to allocate memory specifically within the LibxmlContext memory context. This ensures that all libxml memory allocations are properly managed within PostgreSQL's memory management system and can be easily tracked and freed as a group.

## Parameters / Member Variables
- `size`: The number of bytes to allocate

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextAlloc
- Called from:
  - PgXmlErrorContext  
  - xml_memory_init (as callback registration)
  - Indirectly called by libxml through callback mechanism

## Notes and Other Information
- This is a static function used internally within the xml.c module
- Serves as a callback function registered with libxml via xmlMemSetup
- Part of PostgreSQL's integration with libxml for consistent memory management
- All memory allocated through this function will be automatically freed when LibxmlContext is destroyed