# xml_errorHandler

## Location
src/backend/utils/adt/xml.c: 2088 - 2275

## Overview
The central error handler callback function for libxml2 that captures, processes, and formats XML parsing errors according to PostgreSQL's error reporting standards.

## Definition


## Detailed Description
xml_errorHandler is a comprehensive error handler that serves as the bridge between libxml2's error system and PostgreSQL's error reporting infrastructure. It processes various types of XML errors (parser errors, namespace errors, etc.), applies version-specific compatibility fixes, formats detailed error messages with context information, and either stores them for later reporting or reports them immediately based on severity.

The handler implements sophisticated logic to handle differences between libxml2 versions, suppress certain non-critical warnings, and provide rich context information including line numbers, element names, and file context when available.

## Parameters / Member Variables
- `data`: Pointer to PgXmlErrorContext structure (passed as void* for libxml2 compatibility)
- `error`: PgXmlErrorPtr containing detailed error information from libxml2

## Dependencies
- Functions called/Symbols referenced:
  - PgXmlErrorPtr, PgXmlErrorContext structures
  - ERRCXT_MAGIC (validation constant)
  - PG_XML_STRICTNESS_WELLFORMED, PG_XML_STRICTNESS_LEGACY (strictness levels)
  - makeStringInfo, appendStringInfo, appendStringInfoLineSeparator
  - appendBinaryStringInfo, destroyStringInfo, chopStringInfoNewlines
  - xmlParserPrintFileContext, xmlSetGenericErrorFunc (libxml2 functions)
  - ereport, errmsg_internal, WARNING, NOTICE (PostgreSQL error system)
- Called from (representative examples):
  - Registered as callback in pg_xml_init
  - Used by XmlTable functions
  - Referenced in PgXmlErrorContext structure

## Notes and Other Information
- Implements cross-version compatibility for libxml2 behavior differences
- Handles special cases like XML_ERR_NOT_WELL_BALANCED suppression to avoid redundant errors
- Suppresses XML_WAR_UNDECLARED_ENTITY warnings to avoid DTD-related issues
- Uses sophisticated context formatting by temporarily hijacking libxml2's generic error handler
- Supports legacy mode for backward compatibility with the deprecated xml2 contrib module
- Errors are buffered rather than immediately reported to avoid leaving libxml2 in inconsistent state
- Warnings and notices are reported immediately since they don't cause longjmp() out of libxml2
- Forces backend exit (FATAL) if called with invalid context to prevent undefined behavior