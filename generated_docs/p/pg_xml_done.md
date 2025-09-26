# pg_xml_done

## Location
[src/backend/utils/adt/xml.c:1292-1339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1292-L1339)

## Overview
Restores previous libxml error handling state and cleans up XML error context allocated by pg_xml_init().

## Definition

```c
struct as invalid, just in case somebody somehow manages to
	 * call xml_errorHandler or xml_ereport with it.
	 */
	errcxt->magic = 0;
```
## Detailed Description
The pg_xml_done function is responsible for cleaning up and restoring the global libxml error handling state to what it was before pg_xml_init() was called. This function is essential for proper resource management in PostgreSQL's XML processing subsystem.

The function performs several critical operations:
1. Validates the error context structure using a magic number check
2. Verifies that all pending errors have been handled (in assert-enabled builds)
3. Checks that libxml's global state is synchronized with PostgreSQL's expectations
4. Restores the previously saved error handlers and entity loader
5. Invalidates the error context structure to prevent reuse
6. Releases allocated memory for the error buffer and context

The function includes safety checks to ensure proper error handling state management and warns if libxml's global state appears to be out of sync.

## Parameters / Member Variables
- : Pointer to the PgXmlErrorContext structure that was initialized by pg_xml_init()
- : Boolean flag indicating whether this cleanup is happening during error recovery

## Dependencies
- Functions called/Symbols referenced:
  - [PgXmlErrorContext](../P/PgXmlErrorContext.md) (structure type)
  - ERRCXT_MAGIC (magic number constant)
  - xmlStructuredErrorContext (libxml global variable, if HAVE_XMLSTRUCTUREDERRORCONTEXT is defined)
  - xmlGenericErrorContext (libxml global variable, fallback)
  - xmlSetStructuredErrorFunc (libxml function)
  - xmlSetExternalEntityLoader (libxml function)
  - elog (PostgreSQL logging function)
  - [pfree](pfree.md) (PostgreSQL memory deallocation function)

- Called from (representative examples):
  - [xmltotext_with_options](../x/xmltotext_with_options.md)
  - [xmlelement](../x/xmlelement.md)
  - [xml_parse](../x/xml_parse.md)
  - [map_sql_value_to_xml_value](../m/map_sql_value_to_xml_value.md)
  - [xpath_internal](../x/xpath_internal.md)
  - [XmlTableInitOpaque](../X/XmlTableInitOpaque.md)
  - [XmlTableDestroyOpaque](../X/XmlTableDestroyOpaque.md)

## Notes and Other Information
- This function must be called to properly clean up after pg_xml_init(), typically in PG_TRY/PG_CATCH blocks
- The function includes runtime checks for libxml state synchronization beyond just assertions
- Memory cleanup is performed for both the error buffer and the context structure itself
- The magic number invalidation prevents accidental reuse of deallocated context structures
- Error recovery scenarios are handled differently - pending errors are allowed during error conditions