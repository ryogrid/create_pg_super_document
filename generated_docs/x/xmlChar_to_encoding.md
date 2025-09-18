# xmlChar_to_encoding

## Location
src/backend/utils/adt/xml.c: 251 - 272

## Overview
Converts an XML character encoding name to PostgreSQL's internal encoding identifier, providing validation and error handling for encoding names.

## Definition


## Detailed Description
This function serves as a wrapper around PostgreSQL's  function, specifically designed for XML processing. It takes an XML character encoding name (as xmlChar*) and attempts to convert it to PostgreSQL's internal encoding identifier. If the encoding name is invalid or unsupported, the function reports an error rather than returning a failure code, ensuring that XML processing operations fail cleanly when encountering unsupported encodings.

## Parameters / Member Variables
- : An xmlChar pointer containing the name of the character encoding to be converted (e.g., "UTF-8", "ISO-8859-1")

## Dependencies
- Functions called/Symbols referenced:
  - pg_char_to_encoding
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - [xml_recv](xml_recv.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the xml.c file
- The function performs error checking and throws a PostgreSQL error (ERRCODE_INVALID_PARAMETER_VALUE) instead of returning an error code
- It bridges the gap between libxml2's xmlChar type and PostgreSQL's standard C string handling
- Returns the internal encoding identifier on success, never returns on failure due to ereport(ERROR)