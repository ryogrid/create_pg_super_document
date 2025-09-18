# xml_is_document

## Location
[src/backend/utils/adt/xml.c:1129-1164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1129-L1164)

## Overview
The xml_is_document function checks whether a given XML value represents a well-formed XML document as opposed to just an XML content fragment.

## Definition
```c
bool xml_is_document(xmltype *arg)
```

## Detailed Description
This function determines if the provided XML value is a complete, well-formed XML document. It works by attempting to parse the XML using the document parsing mode and checking if any errors occur during parsing. The function uses PostgreSQL's error context system to catch parsing errors without propagating them as exceptions. If the XML can be successfully parsed as a document without errors, the function returns true; otherwise, it returns false. The function properly cleans up any allocated XML document structures after parsing.

## Parameters / Member Variables
- `arg`: The XML value to check for document validity

## Dependencies
- Functions called/Symbols referenced:
  - [ErrorSaveContext](../E/ErrorSaveContext.md) (error handling context structure)
  - xml_parse (core XML parsing function)
  - XMLOPTION_DOCUMENT (parsing option for document mode)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (database encoding retrieval)
  - xmlFreeDoc (libxml2 document cleanup function)
  - NO_XML_SUPPORT (fallback when libxml2 not available)
- Called from (representative examples):
  - [ExecEvalXmlExpr](../E/ExecEvalXmlExpr.md) (in XML expression evaluation)

## Notes and Other Information
- This function is only available when PostgreSQL is compiled with libxml2 support (USE_LIBXML)
- When libxml2 support is not available, the function calls NO_XML_SUPPORT() and returns false
- The function uses soft error handling to avoid throwing exceptions for invalid XML
- Used internally by PostgreSQL's XML processing system for the XMLISVALID SQL function
- The function specifically tests for document validity, not just well-formedness of XML content
- Memory management is handled properly by freeing the parsed document structure if parsing succeeds