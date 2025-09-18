# xmlexists

## Location
src/backend/utils/adt/xml.c: 4544 - 4566

## Overview
Determines if the node specified by the supplied XPath exists in a given XML document, returning a boolean value.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that evaluates an XPath expression against an XML document and returns a boolean indicating whether any matching nodes exist. This function provides a way to test for the existence of XML elements or attributes without retrieving the actual data. It is part of PostgreSQL's XML processing capabilities and requires libxml2 support to function.

The function internally uses the  function to perform the XPath evaluation and simply checks if any results were returned (res_nitems > 0) to determine the boolean result.

## Parameters / Member Variables
- : XPath expression as text - the XPath query to evaluate against the XML document
- : XML data - the XML document to search within

## Dependencies
- Functions called/Symbols referenced:
  - [xpath_internal](xpath_internal.md)
  - PG_GETARG_TEXT_PP
  - PG_GETARG_XML_P
  - PG_RETURN_BOOL
  - NO_XML_SUPPORT
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Requires libxml2 support (USE_LIBXML macro must be defined)
- Returns NO_XML_SUPPORT error when libxml2 is not available
- Located in src/backend/utils/adt/xml.c:4544-4566
- Part of PostgreSQL's XML data type support
- Returns true if one or more nodes match the XPath expression, false otherwise