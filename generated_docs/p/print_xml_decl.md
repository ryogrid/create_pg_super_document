# print_xml_decl

## Location
src/backend/utils/adt/xml.c: 1606 - 1671

## Overview
Generates and appends an XML declaration to a StringInfo buffer following SQL standard serialization rules, avoiding unnecessary declarations when possible.

## Definition


## Detailed Description
This function implements the "Serialization of an XML value" clause from the SQL standard by generating XML declarations only when necessary. The function follows a minimalist approach to avoid cluttering simple XML output with redundant declarations.

The decision logic for generating a declaration:
- Always generate if standalone property is specified (not -1)
- Always generate if encoding is specified and not UTF-8
- Always generate if version is specified and not "1.0" (the default)
- Otherwise, omit the declaration to keep output clean

When a declaration is generated, it follows XML 1.0 specification format with proper quoting and attribute ordering. The version attribute is always included (as required by XML), defaulting to "1.0" if not specified.

## Parameters / Member Variables
- : StringInfo buffer to append the XML declaration to
- : XML version string (typically "1.0"), or NULL to use default
- : PostgreSQL encoding identifier for the document encoding
- : Standalone flag (1=yes, 0=no, -1=not specified/omit)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (string comparison for version check)
  - PG_XML_DEFAULT_VERSION (default XML version constant "1.0")
  - PG_UTF8 (UTF-8 encoding identifier constant)
  - appendStringInfoString (append string to StringInfo buffer)
  - appendStringInfo (formatted append to StringInfo buffer)  
  - pg_encoding_to_char (convert PostgreSQL encoding to string name)
- Called from (representative examples):
  - xml_out_internal (XML output processing)
  - xmlconcat (XML concatenation operations)
  - xmlroot (XML root element processing)

## Notes and Other Information
- Returns true if declaration was written, false if omitted
- Function is static (internal to xml.c file)
- Implements SQL:2003 standard behavior for XML serialization
- Uses double quotes for all attribute values in the declaration
- Encoding names use PostgreSQL internal names (may want IANA names in future)
- Minimizes XML declaration generation to avoid verbose output for simple cases
- Always includes version attribute when declaration is present (XML requirement)
- UTF-8 encoding is considered default and doesn't trigger declaration generation