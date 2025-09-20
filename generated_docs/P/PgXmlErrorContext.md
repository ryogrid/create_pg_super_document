# PgXmlErrorContext

## Location
[src/backend/utils/adt/xml.c:117-194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L117-L194)

## Overview
A context structure for managing XML parsing errors and maintaining libxml error handling state during PostgreSQL XML operations.

## Definition

```c
struct PgXmlErrorContext
{
	int			magic;
	/* strictness argument passed to pg_xml_init */
	PgXmlStrictness strictness;
	/* current error status and accumulated message, if any */
	bool		err_occurred;
	StringInfoData err_buf;
	/* previous libxml error handling state (saved by pg_xml_init) */
	xmlStructuredErrorFunc saved_errfunc;
	void	   *saved_errcxt;
	/* previous libxml entity handler (saved by pg_xml_init) */
	xmlExternalEntityLoader saved_entityfunc;
};
```
## Detailed Description
PgXmlErrorContext is a critical structure that manages error handling during XML operations in PostgreSQL. It serves as a bridge between PostgreSQL's error reporting system and libxml's error handling mechanisms. The structure maintains the current error state, accumulates error messages, and preserves the previous libxml error handling configuration to allow proper restoration after XML operations complete.

The structure is designed to support different levels of XML parsing strictness through the PgXmlStrictness enumeration, allowing PostgreSQL to handle XML errors according to the specified tolerance level (legacy, well-formed, or strict).

## Parameters / Member Variables
- `magic`: Magic number for structure validation and debugging purposes
- `strictness`: Specifies the level of XML parsing strictness (PG_XML_STRICTNESS_LEGACY, PG_XML_STRICTNESS_WELLFORMED, or PG_XML_STRICTNESS_ALL)
- `err_occurred`: Boolean flag indicating whether an XML parsing error has occurred
- `err_buf`: StringInfo buffer that accumulates error messages during XML processing
- `saved_errfunc`: Stores the previous libxml structured error function pointer for restoration
- `*saved_errcxt`: Stores the previous libxml error context for restoration
- `saved_entityfunc`: Stores the previous libxml external entity loader function for restoration

## Dependencies
- Functions called/Symbols referenced:
  - PgXmlStrictness (enum for strictness levels)
  - [StringInfoData](../S/StringInfoData.md) (PostgreSQL string buffer)
  - xmlStructuredErrorFunc (libxml error function type)
  - xmlExternalEntityLoader (libxml entity loader type)
- Called from (representative examples):
  - [pg_xml_init](../p/pg_xml_init.md) (initializes XML context)
  - pg_xml_done (cleans up XML context)
  - xml_errorHandler (handles XML parsing errors)
  - xml_errsave (saves XML parsing errors)
  - [XmlTableBuilderData](../X/XmlTableBuilderData.md) (contains reference to this context)

## Notes and Other Information
- This structure is private to xml.c and not exposed in public headers
- The magic field is used for debugging and validation purposes
- Error accumulation allows multiple XML errors to be collected and reported together
- The saved_* fields are crucial for proper cleanup and restoration of libxml state
- Used extensively in XML table functions and general XML processing operations
- Thread safety considerations apply when libxml state is modified