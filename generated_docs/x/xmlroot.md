# xmlroot

## Location
[src/backend/utils/adt/xml.c:1063-1118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1063-L1118)

## Overview
The xmlroot function modifies the XML declaration of an existing XML document, allowing changes to the version and standalone attributes while preserving the document content.

## Definition
```c
xmltype *xmlroot(xmltype *data, text *version, int standalone)
```

## Detailed Description
This function takes an existing XML document and creates a new XML document with a modified XML declaration. It parses the original XML declaration to extract current version and standalone values, then replaces them with new values as specified by the parameters. The function handles various standalone options including explicitly setting it to 'yes' or 'no', omitting the standalone attribute entirely, or preserving the original value. The document content after the XML declaration remains unchanged.

## Parameters / Member Variables
- `data`: The input XML document whose declaration will be modified
- `version`: New version string for the XML declaration (if NULL, preserves original version)  
- `standalone`: Specifies the standalone attribute behavior using XML_STANDALONE_* constants:
  - XML_STANDALONE_YES: Sets standalone="yes"
  - XML_STANDALONE_NO: Sets standalone="no"  
  - XML_STANDALONE_NO_VALUE: Sets standalone attribute to no value
  - XML_STANDALONE_OMITTED: Preserves the original standalone value

## Dependencies
- Functions called/Symbols referenced:
  - VARSIZE
  - text_to_cstring
  - parse_xml_decl
  - xml_text2xmlChar
  - print_xml_decl
  - [stringinfo_to_xmltype](../s/stringinfo_to_xmltype.md)
  - XML_STANDALONE_* constants
- Called from (representative examples):
  - [ExecEvalXmlExpr](../E/ExecEvalXmlExpr.md) (in XML expression evaluation)

## Notes and Other Information
- This function is only available when PostgreSQL is compiled with libxml2 support (USE_LIBXML)
- When libxml2 support is not available, the function calls NO_XML_SUPPORT() and returns NULL
- The function creates a completely new XML document rather than modifying the original in-place
- Used internally by PostgreSQL's XML processing system for XMLROOT SQL function implementation