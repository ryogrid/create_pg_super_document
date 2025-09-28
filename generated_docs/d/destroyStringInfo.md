# destroyStringInfo

## Location
[src/common/stringinfo.c:361-368](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/stringinfo.c#L361-L368)

## Overview
Frees a StringInfo structure and its associated buffer memory, serving as the opposite operation to makeStringInfo().

## Definition

```c
void
destroyStringInfo(StringInfo str)
```
## Detailed Description
This function completely deallocates a StringInfo structure, freeing both the data buffer and the StringInfo structure itself. It is designed to be used only with StringInfo objects that were allocated using palloc (typically created via makeStringInfo()).

The function performs a safety check to ensure that only writable StringInfo structures are destroyed (read-only StringInfos have maxlen == 0 and should not be destroyed). It then frees the data buffer first, followed by the StringInfo structure itself.

This function should be called when a StringInfo is no longer needed to prevent memory leaks. It's particularly important in long-running processes where temporary StringInfo objects are created and discarded frequently.

## Parameters / Member Variables
- : The StringInfo structure to destroy (must be palloc'd, not read-only)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (validation macro)
  - [pfree](../p/pfree.md) (memory deallocation function)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md)
  - [check_publications](../c/check_publications.md)
  - [jsonb_send](../j/jsonb_send.md)
  - [xml_errorHandler](../x/xml_errorHandler.md)
  - [freeJsonLexContext](../f/freeJsonLexContext.md)

## Notes and Other Information
- Must only be used with palloc'd StringInfo objects (created via makeStringInfo())
- Cannot be used with read-only StringInfo structures (maxlen == 0)
- Frees both the data buffer and the StringInfo structure itself
- Essential for preventing memory leaks in applications using temporary StringInfo objects
- The function performs validation to prevent destruction of read-only StringInfos
- Memory is freed using pfree(), consistent with PostgreSQL's memory management system

## Simplified Source

```c
// Simplified version of destroyStringInfo
void destroyStringInfo(StringInfo str) {
    // Prevent destruction of read-only StringInfos
    Assert(str->maxlen != 0);

    // Free data buffer and structure
    pfree(str->data);
    pfree(str);
}
```

Key simplifications made:
- Removed detailed comments while preserving essential validation
- Maintained the assertion for read-only StringInfo protection
- Preserved the correct order of memory deallocation
- Kept the simple two-step cleanup process