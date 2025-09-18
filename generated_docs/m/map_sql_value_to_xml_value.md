# map_sql_value_to_xml_value

## Location
[src/backend/utils/adt/xml.c:2478-2696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2478-L2696)

## Overview
Converts SQL values to XML-compliant string representations according to SQL/XML:2008 section 9.8, with special formatting for various data types and optional character escaping.

## Definition


## Detailed Description
This function converts PostgreSQL Datum values to their XML string representations, implementing the SQL/XML standard specifications. It handles various data types with specialized formatting requirements and provides optional character escaping for string values.

The function provides special handling for several data types:
- **Arrays**: Recursively processes each element, wrapping them in  tags
- **Boolean**: Converts to "true" or "false" strings (XSD format)
- **Date**: Formats using XSD date format, rejecting infinite values
- **Timestamp/TimestampTZ**: Formats using XSD datetime format with timezone support
- **Bytea**: Encodes as Base64 or BinHex (requires libxml2)
- **Other types**: Uses the type's native text output function

When  is true, special XML characters (&, <, >, etc.) are escaped to entity references. This parameter is typically false when used with libxml2 functions that perform their own escaping.

## Parameters
- : The Datum value to convert to XML format
- : The OID of the PostgreSQL data type
- : When true, escapes special XML characters in string values; when false, leaves them unescaped (useful with libxml2 functions that do their own escaping)

## Dependencies
- Functions called/Symbols referenced:
  - : Check if type is an array or array domain
  - : Extract array from Datum
  - : Get array element type
  - : Get type characteristics
  - : Break array into individual elements
  - : Get base type (flatten domains)
  - , , : Type-specific Datum extractors
  - : Convert Julian date to calendar date
  - , : Format dates/timestamps
  - : Convert timestamp to time structure
  - : Extract bytea from Datum
  - , : XML error context management
  - , : libxml2 buffer operations
  - , : Binary encoding functions
  - , : Generic type output
  - : XML character escaping function

- Called from (representative examples):
  - : XML expression evaluation in executor
  - : XML element construction
  - : Converting SQL rows to XML elements
  - : XPath result processing
  - Self-recursive calls for array element processing

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Supports both libxml2 and non-libxml2 builds (bytea encoding only available with libxml2)
- Rejects infinite date/timestamp values as they are not supported by XSD
- Uses XSD-compliant formatting for date/time types ()
- The  global variable controls whether bytea is encoded as Base64 or BinHex
- Properly handles PostgreSQL domains by flattening them to their base types
- XML type values are passed through unchanged to avoid double-processing
- Array processing uses recursive calls with forced string escaping enabled