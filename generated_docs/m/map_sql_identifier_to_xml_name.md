# map_sql_identifier_to_xml_name

## Location
[src/backend/utils/adt/xml.c:2380-2435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2380-L2435)

## Overview
Maps SQL identifiers to XML names according to SQL/XML:2008 section 9.1, performing necessary character escaping to ensure valid XML naming conventions.

## Definition

```c
char *
map_sql_identifier_to_xml_name(const char *ident, bool fully_escaped,
							   bool escape_period)
```
## Detailed Description
This function converts SQL identifiers to XML-compliant names by escaping special characters that are invalid in XML names. The function follows the SQL/XML standard specification for identifier mapping, ensuring that the resulting XML names are valid while preserving the original identifier's meaning through reversible character encoding.

The function performs several types of character escaping:
- Colons () are escaped as 
- Underscores followed by 'x' () are escaped as  to avoid conflicts with the escape sequence format
- Identifiers starting with "xml" (case-insensitive) have their first character escaped to avoid XML reserved names
- Periods () can be optionally escaped as  when  is true
- Any character that is invalid in XML names is escaped using Unicode hex notation 

## Parameters
- : The input SQL identifier string to be converted
- : When true, applies full escaping including colon and "xml" prefix handling
- : When true, escapes period characters (requires  to be false)

## Dependencies
- Functions called/Symbols referenced:
  - : Initialize string buffer for building result
  - : Append escape sequences to result buffer
  - : Append formatted Unicode escape sequences
  - : Append original character bytes when no escaping needed
  - : Get multibyte character length for proper iteration
  - : Case-insensitive string comparison for "xml" prefix detection
  - : Convert SQL character to Unicode code point
  - : Check if character is valid as first character in XML name
  - : Check if character is valid in XML name (non-first position)
  - : Macro for non-libxml builds

- Called from (representative examples):
  - : XML expression transformation in parser
  - : Converting query results to XML
  - : Converting schema information to XML
  - : Handling multi-part identifiers
  - : Converting SQL rows to XML elements

## Notes and Other Information
- Only available when compiled with  support
- The function includes an assertion that  and  cannot both be true
- Returns a newly allocated string that must be freed by the caller
- Supports multibyte character encodings through proper use of 
- The escaping is designed to be reversible through the corresponding  function