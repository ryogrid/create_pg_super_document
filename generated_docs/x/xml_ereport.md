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
  - [PgXmlErrorContext](../P/PgXmlErrorContext.md) (structure)
  - ERRCXT_MAGIC (validation constant)
  - [errdetail_internal](../e/errdetail_internal.md) (PostgreSQL error reporting)
  - elog, ereport, errcode, errmsg_internal (PostgreSQL error system)
- Called from (representative examples):
  - [xmltotext_with_options](xmltotext_with_options.md) (multiple calls)
  - [xmlelement](xmlelement.md)
  - [xml_parse](xml_parse.md)
  - [xpath_internal](xpath_internal.md)
  - XmlTable* functions

## Notes and Other Information
- This function is exported for use by extension modules that need to share the core libxml error handler
- Requires pg_xml_init() to have been called previously to set up the error context
- Automatically resets the err_occurred flag after reporting to prevent double-reporting
- Only includes libxml detail information if the error buffer contains text
- Used extensively throughout PostgreSQL's XML functionality for consistent error reporting

## Simplified Source

```c
void
xml_ereport(PgXmlErrorContext *errcxt, int level, int sqlcode, const char *msg)
{
    char *detail;

    // Validate error context structure
    if (errcxt->magic != ERRCXT_MAGIC)
        elog(ERROR, "xml_ereport called with invalid PgXmlErrorContext");

    // Mark error as processed
    errcxt->err_occurred = false;

    // Use libxml error details if available
    if (errcxt->err_buf.len > 0)
        detail = errcxt->err_buf.data;
    else
        detail = NULL;

    // Report error with SQL message and optional libxml details
    ereport(level,
            (errcode(sqlcode),
             errmsg_internal("%s", msg),
             detail ? errdetail_internal("%s", detail) : 0));
}
```