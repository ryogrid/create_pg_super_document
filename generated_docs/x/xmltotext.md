# xmltotext

## Location
[src/backend/utils/adt/xml.c:646-655](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L646-L655)

## Overview
Converts an XML value to a text value through binary-compatible casting, providing a simple interface to extract the textual content from XML data.

## Definition
Datum xmltotext(PG_FUNCTION_ARGS)

## Detailed Description
The xmltotext function is a PostgreSQL SQL function that performs a simple binary-compatible conversion from XML data type to text data type. It retrieves the XML argument using PG_GETARG_XML_P(0) and directly casts it to text format using PG_RETURN_TEXT_P. The function relies on the fact that XML and text types are binary compatible in PostgreSQL internal representation, so no actual data transformation is needed.

This function provides the basic XML-to-text conversion functionality without any formatting options. For more advanced conversion with formatting and indentation options, the xmltotext_with_options function should be used instead.

## Parameters / Member Variables
- Function takes one argument (accessible via PG_FUNCTION_ARGS):
  - XML data (xmltype): The XML value to be converted to text

## Dependencies
- Functions called/Symbols referenced:
  - [xmltype](xmltype.md) (data type)
  - PG_GETARG_XML_P (macro for retrieving XML argument)
  - PG_RETURN_TEXT_P (macro for returning text result)
- Called from (representative examples):
  - Direct SQL function calls through PostgreSQL function call interface

## Notes and Other Information
- The function performs a simple binary cast without any content processing
- No libxml2 functionality is required for this basic conversion
- The comment "It is actually binary compatible" indicates the implementation relies on internal type compatibility
- This is the simpler version of XML-to-text conversion; xmltotext_with_options provides more sophisticated formatting capabilities
- Located in src/backend/utils/adt/xml.c at lines 646-655