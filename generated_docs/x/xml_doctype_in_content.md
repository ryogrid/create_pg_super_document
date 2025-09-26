# xml_doctype_in_content

## Location
[src/backend/utils/adt/xml.c:1672-1747](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1672-L1747)

## Overview
Detects whether an XML CONTENT input contains a Document Type Declaration (DTD), enabling SQL/XML:2006+ compliant parsing behavior.

## Definition

```c
static bool
xml_doctype_in_content(const xmlChar *str)
```
## Detailed Description
This function implements a compatibility layer between SQL/XML:2003 and SQL/XML:2006+ standards for CONTENT parsing. The SQL/XML:2003 definition of CONTENT ("XMLDecl? content") excludes documents with DTDs, creating an inconsistency where CONTENT is not a proper superset of DOCUMENT. SQL/XML:2006 fixed this by redefining CONTENT to accept any valid DOCUMENT.

Since libxml2 only supports the 2003 behavior (rejecting DTDs in CONTENT mode), this function provides early DTD detection. When a DTD is found, the parsing can be switched to DOCUMENT mode to achieve the more permissive 2006+ behavior.

The function scans the input looking for a DOCTYPE declaration, properly handling the XML constructs that can legally precede it:
- XML declaration (handled by caller via parse_xml_decl)
- Whitespace
- Comments (<!-- ... -->)
- Processing instructions (<? ... ?>)

The function is designed to be conservative - it returns false for any malformed or unrecognized input, allowing normal CONTENT parsing with proper libxml2 error reporting.

## Parameters / Member Variables
- : UTF-8 encoded XML string to examine for DTD presence

## Dependencies
- Functions called/Symbols referenced:
  - SKIP_XML_SPACE (macro for skipping XML whitespace)
  - xmlStrncmp (libxml2 string comparison function)
  - xmlStrstr (libxml2 string search function)
- Called from (representative examples):
  - [xml_parse](xml_parse.md) (main XML parsing function)

## Notes and Other Information
- Returns true if DOCTYPE declaration is detected, false otherwise
- Function is static (internal to xml.c file)  
- Designed for use only after pg_xml_init() has been called
- Input must already be in UTF-8 encoding
- Handles XML comments and processing instructions correctly
- Conservative approach: returns false for any questionable input
- Enables SQL/XML:2006+ compliant CONTENT parsing in PostgreSQL
- DTD detection is optimized for typical cases (DTD near document start)
- Part of PostgreSQL's XML standards compliance implementation