# pg_xml_init

## Location
src/backend/utils/adt/xml.c: 1211 - 1291

## Overview
The pg_xml_init function initializes libxml2 with comprehensive error handling support and establishes PostgreSQL's custom error handler and entity loader for secure XML processing.

## Definition
```c
PgXmlErrorContext *pg_xml_init(PgXmlStrictness strictness)
```

## Detailed Description
This function provides a complete initialization setup for libxml2 operations that require error handling. It first calls pg_xml_init_library() for basic library initialization, then creates and configures a custom error handling context. The function installs PostgreSQL's structured error handler to capture and manage libxml2 errors according to the specified strictness level. It also sets up a custom entity loader to prevent security vulnerabilities by blocking unauthorized access to external files and URLs. The function includes compatibility verification to ensure the error context is properly established, which protects against ABI mismatches between compile-time and runtime libxml2 versions.

## Parameters / Member Variables
- `strictness`: Determines the level of error reporting and which errors are treated as fatal vs. warnings (PgXmlStrictness enum)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_xml_init_library](pg_xml_init_library.md) (basic libxml2 initialization)
  - [palloc](palloc.md) (PostgreSQL memory allocation)
  - initStringInfo (string buffer initialization)
  - xmlSetStructuredErrorFunc (libxml2 error handler setup)
  - xml_errorHandler (PostgreSQL's custom error handler)
  - xmlGetExternalEntityLoader (get current entity loader)
  - xmlSetExternalEntityLoader (set custom entity loader)
  - xmlPgEntityLoader (PostgreSQL's secure entity loader)
  - ERRCXT_MAGIC (error context validation constant)
- Called from (representative examples):
  - [xmltotext_with_options](../x/xmltotext_with_options.md) (XML to text conversion)
  - [xmlelement](../x/xmlelement.md) (XML element creation)
  - xml_parse (XML parsing operations)
  - [xpath_internal](../x/xpath_internal.md) (XPath evaluation)

## Notes and Other Information
- This function MUST be paired with pg_xml_done() in a PG_TRY block to ensure proper cleanup
- The function creates a PgXmlErrorContext structure that tracks error state and provides error message buffering
- Includes runtime compatibility checking to detect libxml2 ABI mismatches that static version checks cannot catch
- The custom entity loader prevents security vulnerabilities by blocking access to external files and URLs
- Error strictness levels control whether certain XML issues are treated as errors or warnings
- Used extensively throughout PostgreSQL's XML processing subsystem for any operation requiring error handling
- The function is exported for use by contrib/xml2 and other extensions that need libxml2 error handling