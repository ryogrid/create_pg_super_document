# xml_errsave

## Location
[src/backend/utils/adt/xml.c:2059-2087](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2059-L2087)

## Overview
A soft error handling function that saves XML-related errors to an ErrorSaveContext instead of immediately throwing them, enabling recoverable error processing.

## Definition

```c
struct */
	if (errcxt->magic != ERRCXT_MAGIC)
		elog(ERROR, "xml_errsave called with invalid PgXmlErrorContext");
```
## Detailed Description
xml_errsave provides a mechanism for handling XML errors in a recoverable manner. Unlike xml_ereport which immediately throws errors, this function can save error details into an ErrorSaveContext, allowing the calling code to continue execution and handle the error gracefully. If no ErrorSaveContext is provided, it falls back to throwing the error like xml_ereport with ERROR level.

This function is particularly useful for operations where transaction abort cleanup is not necessary and where the caller wants to handle XML processing errors without disrupting the entire operation.

## Parameters / Member Variables
- `escontext`: Node pointer that may be an ErrorSaveContext for soft error handling
- `errcxt`: Pointer to PgXmlErrorContext containing captured libxml error details  
- `sqlcode`: PostgreSQL error code to report
- `msg`: Primary SQL-level error message

## Dependencies
- Functions called/Symbols referenced:
  - PgXmlErrorContext (structure)
  - ERRCXT_MAGIC (validation constant)
  - errsave (PostgreSQL soft error system)
  - errdetail_internal (PostgreSQL error reporting)
  - elog, errcode, errmsg_internal (PostgreSQL error system)
- Called from (representative examples):
  - xml_parse (multiple calls for different parsing scenarios)
  - Referenced in PgXmlErrorContext structure

## Notes and Other Information
- This is a static function only used within the xml.c module
- Should only be used for errors that don't require transaction abort for cleanup
- Automatically resets the err_occurred flag after processing
- Part of PostgreSQL's soft error handling infrastructure introduced for better error recovery
- If escontext is not an ErrorSaveContext, behaves identically to xml_ereport with ERROR level