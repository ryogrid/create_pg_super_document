# xml_text2xmlChar

## Location
src/backend/utils/adt/xml.c: 1933 - 1945

## Overview
A simple conversion utility function that transforms PostgreSQL's text type to libxml2's xmlChar string format.

## Definition


## Detailed Description
This function provides a straightforward interface for converting PostgreSQL's variable-length text data type to the null-terminated xmlChar string format required by libxml2 functions. It serves as a bridge between PostgreSQL's internal text representation and libxml2's expected string format.

The function essentially extracts the string content from PostgreSQL's text type (which includes a length header) and returns it as a null-terminated C string cast to xmlChar*. This conversion is necessary because libxml2 functions expect null-terminated strings, while PostgreSQL's text type stores both length information and data.

The conversion handles the TOAST (The Oversized-Attribute Storage Technique) decompression implicitly through text_to_cstring, ensuring that even large XML documents stored in compressed form are properly accessible.

## Parameters / Member Variables
- : PostgreSQL text value containing XML string data

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring (PostgreSQL function to convert text to null-terminated C string)
- Called from (representative examples):
  - xmltext (convert XML to text)
  - xmltotext_with_options (XML to text conversion with options)
  - xmlroot (XML root element processing)
  - xml_parse (main XML parsing function)

## Notes and Other Information
- Returns xmlChar* (libxml2's string type, equivalent to unsigned char*)
- Function is static (internal to xml.c file)
- Part of the xmlChar<->text conversion utility functions
- Simple wrapper around text_to_cstring with appropriate type casting
- Handles TOAST decompression automatically through text_to_cstring
- Result is a null-terminated string suitable for libxml2 functions
- Memory management follows PostgreSQL conventions (result is palloc'd)
- Essential for interfacing PostgreSQL text data with libxml2 API