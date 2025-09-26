# parse_xml_decl

## Location
src/backend/utils/adt/xml.c: 1433 - 1605

## Overview
Parses an XML declaration from the beginning of an XML document string, extracting version, encoding, and standalone attributes according to XML specification.

## Definition


## Detailed Description
This function parses an XML declaration (<?xml ... ?>) from the start of an XML document string. It validates the syntax according to XML standards and extracts the three optional attributes: version, encoding, and standalone. The function is designed to be lenient - it will succeed even if the XML declaration is not present, but will fail if a malformed declaration is found.

The parsing follows XML 1.0 specification rules:
- The declaration must start with exactly "<?xml"
- Version attribute is required if declaration is present
- Encoding and standalone attributes are optional
- Attributes must be properly quoted with single or double quotes
- Whitespace handling follows XML rules
- Declaration must end with "?>"

The function also validates that all characters in the parsed declaration are ASCII (values ≤ 127), ensuring compatibility with XML processing requirements.

## Parameters / Member Variables
- : Input XML string to parse (null-terminated xmlChar string)
- : Output parameter for length of parsed declaration (can be NULL if not needed)
- : Output parameter for XML version string (locally palloc'd, can be NULL if not wanted)
- : Output parameter for encoding string (locally palloc'd, can be NULL if not wanted)  
- : Output parameter for standalone flag (1=yes, 0=no, -1=not specified, can be NULL if not wanted)

## Dependencies
- Functions called/Symbols referenced:
  - pg_xml_init_library (initialize libxml2)
  - strnlen (get string length with limit)
  - xmlGetUTF8Char (get UTF-8 character from libxml2)
  - PG_XMLISNAMECHAR (check if character is valid XML name character)
  - CHECK_XML_SPACE, SKIP_XML_SPACE (XML whitespace handling macros)
  - xml_pnstrdup (PostgreSQL XML string duplication)
  - xmlStrncmp, xmlStrchr (libxml2 string functions)
- Called from (representative examples):
  - xml_out_internal (XML output processing)
  - xml_recv (XML input from binary format)
  - xmlconcat (XML concatenation)
  - xmlroot (XML root element processing)
  - xml_parse (main XML parsing function)

## Notes and Other Information
- Returns 0 (XML_ERR_OK) on success, error codes on failure
- Function is static (internal to xml.c file)
- Safely handles NULL output parameters for unwanted values
- Initializes libxml2 if not already done, making it safe to call independently
- Uses PostgreSQL's memory allocation (palloc) for returned strings
- Validates ASCII-only content in the declaration portion
- Handles both single and double quote delimiters for attribute values
- Distinguishes between absent declaration (success) vs malformed declaration (error)