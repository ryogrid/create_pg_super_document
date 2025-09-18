# xpath

## Location
src/backend/utils/adt/xml.c: 4521 - 4543

## Overview
PostgreSQL SQL function that evaluates XPath expressions against XML documents and returns an array of matching XML values.

## Definition
```c
Datum xpath(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements PostgreSQL's xpath() SQL function, which evaluates XPath expressions against XML documents and returns the results as an array of XML values. It serves as a public SQL interface to the internal xpath_internal function. The function accepts an XPath expression, XML data, and optional namespace mappings, then returns all matching results as a PostgreSQL array. This provides a substitute for XQuery sequences since PostgreSQL doesn't yet support full XQuery sequence functionality. The function is conditionally compiled based on libxml2 availability.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: XPath expression as text
  - Argument 1: XML document as xmltype
  - Argument 2: Namespace mappings as array type

## Dependencies
- Functions called/Symbols referenced:
  - xpath_internal
  - initArrayResult
  - makeArrayResult
  - PG_GETARG_TEXT_PP, PG_GETARG_XML_P, PG_GETARG_ARRAYTYPE_P (PostgreSQL argument extraction)
  - PG_RETURN_DATUM
  - NO_XML_SUPPORT (fallback when libxml2 not available)
- Called from (representative examples):
  - Direct SQL function calls via PostgreSQL's function call mechanism

## Notes and Other Information
- Public SQL function accessible via SQL queries (e.g., SELECT xpath('/path', xmldata, namespaces))
- Conditionally compiled with #ifdef USE_LIBXML - returns error if libxml2 not available
- Returns array of XML values rather than XQuery sequences (which PostgreSQL doesn't support)
- Part of PostgreSQL's SQL/XML standard implementation
- Function signature follows PostgreSQL's V1 calling convention
- Result array elements maintain their XML type for further XML processing
- Provides the primary interface for XPath functionality in PostgreSQL SQL