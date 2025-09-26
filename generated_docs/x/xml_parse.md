# xml_parse

## Location
src/backend/utils/adt/xml.c: 1748 - 1932

## Overview
The core XML parsing function that converts text data to libxml2's internal xmlDoc representation, supporting both DOCUMENT and CONTENT parsing modes with comprehensive error handling.

## Definition

```c
static xmlDocPtr
xml_parse(text *data, XmlOptionType xmloption_arg,
		  bool preserve_whitespace, int encoding,
		  XmlOptionType *parsed_xmloptiontype, xmlNodePtr *parsed_nodes,
		  Node *escontext)
```
## Detailed Description
This function serves as PostgreSQL's central XML parsing engine, bridging between PostgreSQL's text representation and libxml2's DOM representation. It implements sophisticated parsing logic to handle both SQL/XML DOCUMENT and CONTENT modes while providing robust error handling and resource management.

Key parsing behaviors:
- **Encoding handling**: Automatically converts input from specified encoding to UTF-8 for libxml2
- **Mode determination**: Intelligently switches between DOCUMENT and CONTENT parsing based on input characteristics
- **DTD support**: Implements SQL/XML:2006+ compliant parsing by detecting DTDs in CONTENT mode
- **Error management**: Supports both hard errors (ereport) and soft errors (ErrorSaveContext)
- **Resource safety**: Uses PG_TRY/PG_CATCH blocks for proper cleanup

The function first extracts and validates any XML declaration, then determines the appropriate parsing mode. For DOCUMENT mode, it uses xmlCtxtReadDoc with full validation. For CONTENT mode, it uses xmlParseBalancedChunkMemory to parse XML fragments.

## Parameters / Member Variables
- : Source text data to parse (must not be toasted)
- : Requested parsing mode (XMLOPTION_DOCUMENT or XMLOPTION_CONTENT)
- : Whether to preserve whitespace nodes in parsed result
- : Character encoding of the input data
- : Output parameter for actual parsing mode used (can be NULL)
- : Output parameter for parsed node list in CONTENT mode (can be NULL)
- : Error context for soft error handling (can be NULL for hard errors)

## Dependencies
- Functions called/Symbols referenced:
  - xml_text2xmlChar (convert PostgreSQL text to xmlChar string)
  - pg_do_encoding_conversion (convert text encoding to UTF-8)
  - pg_xml_init (initialize XML error context and libxml2)
  - parse_xml_decl (parse and validate XML declaration)
  - xml_doctype_in_content (detect DTD in CONTENT mode)
  - xmlNewParserCtxt, xmlCtxtReadDoc (libxml2 document parsing)
  - xmlParseBalancedChunkMemory (libxml2 fragment parsing)
  - xml_ereport, xml_errsave, errsave (PostgreSQL error reporting)
  - pg_xml_done (cleanup XML error context)
- Called from (representative examples):
  - xml_in (XML input function)
  - xml_recv (XML binary input function)
  - xmlparse (SQL/XML XMLPARSE function)
  - xml_is_document (XML validation function)
  - wellformed_xml (XML well-formedness checking)

## Notes and Other Information
- Returns xmlDocPtr on success, NULL on soft error (caller must check SOFT_ERROR_OCCURRED())
- Function is static (internal to xml.c file)
- Caller is responsible for calling xmlFreeDoc() on returned document to prevent memory leaks
- Automatically promotes CONTENT to DOCUMENT mode when DTD is detected (SQL/XML:2006+ compliance)
- Supports DTD attribute defaults in DOCUMENT mode per SQL/XML:2008 standard
- Uses XML_PARSE_NOBLANKS option when whitespace preservation is disabled
- Handles empty XML content gracefully in CONTENT mode
- Comprehensive resource cleanup in exception handlers prevents memory leaks
- Sets document encoding to UTF-8 and preserves standalone attribute from XML declaration