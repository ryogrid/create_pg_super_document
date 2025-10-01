# xml_pstrdup

## Location
[src/backend/utils/adt/xml.c:1985-2003](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1985-L2003)

## Overview
A string duplication wrapper function that creates a copy of a string within the LibxmlContext memory context for use by libxml operations.

## Definition
```c
static char *xml_pstrdup(const char *string)
```

## Detailed Description
This function serves as a custom string duplication callback for the libxml library. It wraps PostgreSQL's MemoryContextStrdup function to duplicate strings specifically within the LibxmlContext memory context. This ensures that all string allocations performed by libxml are properly managed within PostgreSQL's memory management system and will be automatically freed when the context is destroyed.

## Parameters / Member Variables
- `string`: The null-terminated string to duplicate

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
- Called from:
  - [PgXmlErrorContext](../P/PgXmlErrorContext.md)
  - [xml_memory_init](xml_memory_init.md) (as callback registration)
  - Indirectly called by libxml through callback mechanism

## Notes and Other Information
- This is a static function used internally within the xml.c module
- Serves as a callback function registered with libxml via xmlMemSetup
- Part of PostgreSQL's integration with libxml for consistent memory management
- Returns a newly allocated copy of the input string in the LibxmlContext
- The duplicated string will be automatically freed when LibxmlContext is destroyed
- Uses PostgreSQL's MemoryContextStrdup which handles NULL input appropriately

## Simplified Source

```c
static char *xml_pstrdup(const char *string) {
    return MemoryContextStrdup(LibxmlContext, string);
}
```