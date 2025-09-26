# pg_xml_error_occurred

## Location
[src/backend/utils/adt/xml.c:1340-1354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1340-L1354)

## Overview
Tests whether an XML processing error has occurred in the given error context.

## Definition
```c
bool pg_xml_error_occurred(PgXmlErrorContext *errcxt)
```

## Detailed Description
The pg_xml_error_occurred function provides a simple interface to check whether any XML processing errors have been captured in the specified error context. This function is essential for error handling in PostgreSQL's XML processing subsystem, allowing code to determine if libxml operations have encountered problems.

The function simply returns the value of the err_occurred flag from the error context structure, which is set by PostgreSQL's XML error handlers when libxml reports errors during XML processing operations.

## Parameters / Member Variables
- `errcxt`: Pointer to the PgXmlErrorContext structure to check for error status

## Dependencies
- Functions called/Symbols referenced:
  - PgXmlErrorContext (structure type)

- Called from (representative examples):
  - PG_RETURN_XML_P (macro in xml.h)

## Notes and Other Information
- This is a simple accessor function that provides read-only access to the error state
- The function is typically used after XML processing operations to determine if they succeeded
- The err_occurred flag is set by PostgreSQL's custom XML error handlers when libxml reports errors
- This function should only be called with a valid, initialized PgXmlErrorContext structure