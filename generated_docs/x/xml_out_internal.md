# xml_out_internal

## Location
[src/backend/utils/adt/xml.c:312-355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L312-L355)

## Overview
Internal function that converts PostgreSQL's xml data type to a C string representation, handling XML declaration processing and encoding conversion.

## Definition


## Detailed Description
This static function performs the core logic for converting PostgreSQL's internal xml representation to a C string format. It handles XML declaration parsing and reconstruction, allowing for encoding conversion during output. The function first converts the xml data to a basic string, then when libxml2 is available, it parses any XML declaration to extract version, encoding, and standalone attributes. It reconstructs the declaration with the target encoding and combines it with the remaining XML content. If XML declaration parsing fails, it issues a warning but continues with the basic string conversion.

## Parameters / Member Variables
- : Pointer to the xmltype data to be converted to string representation
- : The PostgreSQL encoding identifier for the desired output encoding

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring
  - parse_xml_decl
  - print_xml_decl
  - initStringInfo
  - appendStringInfoString
  - [pfree](../p/pfree.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg_internal](../e/errmsg_internal.md)
  - errdetail_for_xml_code
- Called from (representative examples):
  - [xml_out](xml_out.md)
  - [xml_send](xml_send.md)
  - [XmlTableSetDocument](../X/XmlTableSetDocument.md)

## Notes and Other Information
- This is a static function, only accessible within xml.c
- Relies on shared representation between xml and text types for base conversion
- When USE_LIBXML is defined, provides sophisticated XML declaration handling
- The function gracefully handles XML declaration parsing failures by issuing warnings
- Memory management: allocates new string buffer for result, caller responsible for freeing
- Handles edge case of removing extra newlines when XML declaration is omitted
- Without libxml2 support, simply returns the basic string conversion
- The target_encoding parameter allows for encoding conversion during output operations