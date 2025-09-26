# xml_pfree

## Location
[src/backend/utils/adt/xml.c:1976-1984](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1976-L1984)

## Overview
A memory deallocation wrapper function that safely frees memory allocated for libxml operations, with NULL pointer safety handling.

## Definition
```c
static void xml_pfree(void *ptr)
```

## Detailed Description
This function serves as a custom memory deallocation callback for the libxml library. It wraps PostgreSQL's pfree function while providing additional safety by handling NULL pointers gracefully. The function includes a NULL check because some parts of libxml assume that calling the free function with NULL is allowed, similar to the standard C library free() function behavior.

## Parameters / Member Variables
- `ptr`: Pointer to the memory block to be freed (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory free function)
- Called from:
  - [PgXmlErrorContext](../P/PgXmlErrorContext.md)
  - [xml_memory_init](xml_memory_init.md) (as callback registration)  
  - Indirectly called by libxml through callback mechanism

## Notes and Other Information
- This is a static function used internally within the xml.c module
- Serves as a callback function registered with libxml via xmlMemSetup
- Part of PostgreSQL's integration with libxml for consistent memory management
- Includes NULL pointer safety check as libxml expects xmlFree(NULL) to be allowed
- Only calls pfree if the pointer is not NULL, preventing potential errors
- Memory freed through this function must have been allocated within the PostgreSQL memory system