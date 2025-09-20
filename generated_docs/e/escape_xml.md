# escape_xml

## Location
[src/backend/utils/adt/xml.c:2697-2728](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2697-L2728)

## Overview
Escapes special XML characters in text strings by replacing them with appropriate XML entity references, ensuring the text is safe for inclusion in XML content.

## Definition

```c
char *
escape_xml(const char *str)
```
## Detailed Description
This function provides XML character escaping functionality that is independent of libxml2, making it available in all PostgreSQL builds. It processes input text character by character and replaces XML-significant characters with their corresponding entity references to prevent XML parsing errors and security issues.

The function performs the following character replacements:
-  (ampersand) →  (must be escaped first to avoid double-escaping)
-  (less-than) →  (prevents interpretation as XML tag start)  
-  (greater-than) →  (prevents interpretation as XML tag end)
-  (carriage return) →  (preserves whitespace formatting)

All other characters are copied unchanged to the output buffer. This focused approach handles the essential XML metacharacters while maintaining performance.

## Parameters
- : The input string to be XML-escaped

## Dependencies
- Functions called/Symbols referenced:
  - : Initialize string buffer for building escaped result
  - : Append entity reference strings to buffer
  - : Efficiently append single characters unchanged

- Called from (representative examples):
  - : XML output formatting in EXPLAIN command
  - : Individual property formatting in EXPLAIN
  - : SQL to XML value conversion
  - : XML node processing
  - : XML table value extraction

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller (via )
- Intentionally independent of libxml2 to ensure availability across all builds
- Does not escape single quotes () or double quotes () as they are safe in XML text content
- The carriage return escaping helps preserve exact whitespace formatting in cross-platform scenarios
- Designed for escaping XML text content, not attribute values (which may require additional escaping)
- Performance optimized with direct character comparisons and efficient string buffer operations