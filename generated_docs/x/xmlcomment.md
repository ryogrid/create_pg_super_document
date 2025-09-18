# xmlcomment

## Location
src/backend/utils/adt/xml.c: 491 - 526

## Overview
Creates an XML comment from a text input, validating that the text doesn't contain invalid comment sequences.

## Definition


## Detailed Description
The xmlcomment function creates a properly formatted XML comment by wrapping the input text with XML comment delimiters ( and ). It performs validation to ensure the input text doesn't contain invalid sequences that would break XML comment syntax:

- Checks for consecutive hyphens () anywhere within the text
- Ensures the text doesn't end with a hyphen ()

The function uses PostgreSQL's internal string manipulation functions to build the final XML comment string and returns it as an XML type. The implementation is conditional on libxml support being available.

## Parameters / Member Variables
- Input text parameter (accessed via ): The text content to be wrapped in XML comment tags

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoText
  - stringinfo_to_xmltype
  - PG_RETURN_XML_P
  - NO_XML_SUPPORT (fallback when libxml not available)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- Function is only available when PostgreSQL is compiled with libxml support ()
- Throws  error for invalid comment content
- XML comments cannot contain  sequences or end with  according to XML specification
- Returns XML type using PostgreSQL's internal XML handling mechanisms