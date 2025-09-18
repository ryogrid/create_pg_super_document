# xmlBuffer_to_xmltype

## Location
src/backend/utils/adt/xml.c: 482 - 490

## Overview
Converts a libxml2 xmlBuffer into a PostgreSQL xmltype value, preserving the exact binary content and length.

## Definition


## Detailed Description
The xmlBuffer_to_xmltype function converts libxml2's xmlBuffer data structure into PostgreSQL's xmltype. This function serves as a bridge between libxml2's memory management system and PostgreSQL's data type system, allowing XML data processed by libxml2 to be safely transferred into PostgreSQL's type system.

The function extracts both the content pointer and length from the xmlBuffer using libxml2's xmlBufferContent and xmlBufferLength functions, then creates a PostgreSQL text value with the exact binary content. Since xmltype is internally represented as text, this conversion preserves all XML data including any special characters or encoding information that libxml2 has processed.

## Parameters / Member Variables
- `buf`: libxml2 xmlBufferPtr containing XML data to be converted

## Dependencies
- Functions called/Symbols referenced:
  - xmlBufferContent (libxml2 function to get buffer content pointer)
  - xmlBufferLength (libxml2 function to get buffer length)
  - cstring_to_text_with_len (creates PostgreSQL text with specified length)
  - [xmltype](xmltype.md) (PostgreSQL XML data type)
- Called from:
  - [xmltotext_with_options](xmltotext_with_options.md) (XML to text conversion with options)
  - [xmlelement](xmlelement.md) (XML element creation)
  - [xml_xmlnodetoxmltype](xml_xmlnodetoxmltype.md) (XML node to xmltype conversion)

## Notes and Other Information
- This is a static function, only available within the xml.c compilation unit
- Requires libxml2 library support (only available when PostgreSQL is compiled with --with-libxml)
- The function preserves the exact binary content from libxml2, including any encoding transformations
- Unlike cstring_to_xmltype, this function works with libxml2's managed buffers rather than plain C strings
- Used primarily in functions that process XML using libxml2's parsing and manipulation capabilities
- The function handles the memory layout differences between libxml2 buffers and PostgreSQL text values
- No validation is performed; the function assumes the xmlBuffer contains valid XML data