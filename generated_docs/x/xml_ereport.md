# xml_ereport

## Location
[src/backend/utils/adt/xml.c:2022-2058](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2022-L2058)

## Overview
A centralized error reporting function that generates PostgreSQL errors for XML-related operations, combining SQL-level messages with detailed libxml error information.

## Definition

```c
struct */
	if (errcxt->magic != ERRCXT_MAGIC)
		elog(ERROR, "xml_ereport called with invalid PgXmlErrorContext");
```
## Detailed Description
xml_ereport is the primary error reporting mechanism for XML operations in PostgreSQL. It takes a SQL-level error message (often from SQL/XML standards) and enhances it with detailed error information captured from libxml2. This function provides a consistent interface for reporting XML errors across all XML functionality in PostgreSQL.

The function validates the error context, resets error state flags, and constructs a comprehensive error report that includes both the high-level SQL message and low-level libxml details when available.

## Parameters / Member Variables
- `errcxt`: Pointer to PgXmlErrorContext containing captured libxml error details
- `level`: Error severity level (ERROR, WARNING, etc.)
- `sqlcode`: PostgreSQL error code to report
- `msg`: Primary SQL-level error message

## Dependencies
- Functions called/Symbols referenced:
  - PgXmlErrorContext (structure)
  - ERRCXT_MAGIC (validation constant)
  - errdetail_internal (PostgreSQL error reporting)
  - elog, ereport, errcode, errmsg_internal (PostgreSQL error system)
- Called from (representative examples):
  - xmltotext_with_options (multiple calls)
  - xmlelement
  - xml_parse
  - xpath_internal
  - XmlTable* functions

## Notes and Other Information
- This function is exported for use by extension modules that need to share the core libxml error handler
- Requires pg_xml_init() to have been called previously to set up the error context
- Automatically resets the err_occurred flag after reporting to prevent double-reporting
- Only includes libxml detail information if the error buffer contains text
- Used extensively throughout PostgreSQL's XML functionality for consistent error reporting