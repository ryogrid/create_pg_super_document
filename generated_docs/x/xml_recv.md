# xml_recv

## Location
src/backend/utils/adt/xml.c: 371 - 437

## Overview
PostgreSQL binary input function that receives XML data from client in binary protocol format, handling encoding detection and conversion with well-formedness validation.

## Definition


## Detailed Description
This function handles the binary protocol reception of XML data from clients. Unlike xml_in which processes text input, xml_recv works with PostgreSQL's binary message protocol. The function performs sophisticated encoding handling by first reading the raw bytes, parsing the XML declaration to detect the specified encoding, and then converting the data to the server's encoding. It defaults to UTF-8 if no encoding is specified in the XML declaration. The function validates well-formedness through XML parsing and performs necessary encoding conversions using PostgreSQL's encoding conversion facilities.

## Parameters / Member Variables
- Takes PostgreSQL function arguments via  macro:
  - Argument 0: StringInfo buffer containing the binary XML data from the client

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER
  - [pq_getmsgbytes](../p/pq_getmsgbytes.md)
  - [palloc](../p/palloc.md)
  - SET_VARSIZE
  - VARDATA
  - parse_xml_decl
  - [xmlChar_to_encoding](xmlChar_to_encoding.md)
  - xml_parse
  - xmlFreeDoc
  - [pg_any_to_server](../p/pg_any_to_server.md)
  - cstring_to_text
  - [pfree](../p/pfree.md)
  - PG_RETURN_XML_P
  - NO_XML_SUPPORT (fallback when libxml2 not available)
- Called from (representative examples):
  - PostgreSQL's binary protocol handling system (indirectly via function registry)

## Notes and Other Information
- This is a PostgreSQL binary I/O function for the xml data type
- Handles encoding detection from XML declarations, defaulting to UTF-8 if not specified
- Performs automatic encoding conversion from client encoding to server encoding
- More sophisticated than xml_in due to binary protocol requirements and encoding handling
- Memory management: allocates temporary buffers and properly cleans up after encoding conversion
- When USE_LIBXML is not defined, calls NO_XML_SUPPORT() macro which reports an error
- The function creates a null-terminated copy for XML declaration parsing
- Validates XML well-formedness before final conversion
- Uses pg_any_to_server for robust encoding conversion between different character sets
- Different from xml_in in that it doesn't rely on client-to-server encoding conversion pipeline