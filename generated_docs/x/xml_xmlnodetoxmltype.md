# xml_xmlnodetoxmltype

## Location
[src/backend/utils/adt/xml.c:4156-4245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L4156-L4245)

## Overview
Converts an XML node to PostgreSQL's xmltype format, handling different node types appropriately by either extracting escaped text content or dumping the entire subtree.

## Definition
```c
static text *xml_xmlnodetoxmltype(xmlNodePtr cur, PgXmlErrorContext *xmlerrcxt)
```

## Detailed Description
This function converts libxml2 xmlNodePtr objects to PostgreSQL's internal xmltype representation. For attribute and text nodes, it extracts and escapes the text content. For all other node types (element, document, etc.), it performs a complete subtree dump using xmlNodeDump. The function handles namespace preservation by creating a copy of the node with its required namespace definitions before dumping. It includes comprehensive error handling using PostgreSQL's PG_TRY/PG_FINALLY exception mechanism and works around libxml2 version-specific issues.

## Parameters / Member Variables
- `cur`: Pointer to the libxml2 XML node to convert
- `xmlerrcxt`: PostgreSQL XML error context for error reporting and handling

## Dependencies
- Functions called/Symbols referenced:
  - [xml_ereport](xml_ereport.md)
  - [xmlBuffer_to_xmltype](xmlBuffer_to_xmltype.md)
  - [escape_xml](../e/escape_xml.md)
  - [cstring_to_text](../c/cstring_to_text.md)
  - PG_TRY/PG_FINALLY/PG_END_TRY (PostgreSQL exception handling)
  - xmlBufferCreate, xmlCopyNode, xmlNodeDump, xmlXPathCastNodeToString (libxml2 functions)
- Called from (representative examples):
  - [xml_xpathobjtoxmlarray](xml_xpathobjtoxmlarray.md)
  - [XmlTableGetValue](../X/XmlTableGetValue.md)

## Notes and Other Information
- Static function used internally by PostgreSQL's XML processing subsystem
- Implements workarounds for libxml2 version-specific bugs (e.g., 2.7.6 XML_DOCUMENT_NODE issues)
- Uses different cleanup strategies (xmlFreeDoc vs xmlFreeNode) based on node type
- Preserves namespace definitions when dumping complex node structures
- Relies on the assumption that XML and TEXT have the same internal representation in PostgreSQL
- Part of the infrastructure supporting XPath operations and XML table functions