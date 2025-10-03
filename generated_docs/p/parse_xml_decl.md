# parse_xml_decl

## Location
[src/backend/utils/adt/xml.c:1433-1605](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1433-L1605)

## Overview
Parses an XML declaration from the beginning of an XML document string, extracting version, encoding, and standalone attributes according to XML specification.

## Definition

```c
static int
parse_xml_decl(const xmlChar *str, size_t *lenp,
			   xmlChar **version, xmlChar **encoding, int *standalone)
```
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
- `*str`: Input XML string to parse (null-terminated xmlChar string)
- `*lenp`: Output parameter for length of parsed declaration (can be NULL if not needed)
- `**version`: Output parameter for XML version string (locally palloc'd, can be NULL if not wanted)
- `**encoding`: Output parameter for encoding string (locally palloc'd, can be NULL if not wanted)
- `*standalone`: Output parameter for standalone flag (1=yes, 0=no, -1=not specified, can be NULL if not wanted)
## Dependencies
- Functions called/Symbols referenced:
  - [pg_xml_init_library](pg_xml_init_library.md) (initialize libxml2)
  - [strnlen](../s/strnlen.md) (get string length with limit)
  - xmlGetUTF8Char (get UTF-8 character from libxml2)
  - PG_XMLISNAMECHAR (check if character is valid XML name character)
  - CHECK_XML_SPACE, SKIP_XML_SPACE (XML whitespace handling macros)
  - [xml_pnstrdup](../x/xml_pnstrdup.md) (PostgreSQL XML string duplication)
  - xmlStrncmp, xmlStrchr (libxml2 string functions)
- Called from (representative examples):
  - [xml_out_internal](../x/xml_out_internal.md) (XML output processing)
  - [xml_recv](../x/xml_recv.md) (XML input from binary format)
  - [xmlconcat](../x/xmlconcat.md) (XML concatenation)
  - [xmlroot](../x/xmlroot.md) (XML root element processing)
  - [xml_parse](../x/xml_parse.md) (main XML parsing function)

## Notes and Other Information
- Returns 0 (XML_ERR_OK) on success, error codes on failure
- Function is static (internal to xml.c file)
- Safely handles NULL output parameters for unwanted values
- Initializes libxml2 if not already done, making it safe to call independently
- Uses PostgreSQL's memory allocation (palloc) for returned strings
- Validates ASCII-only content in the declaration portion
- Handles both single and double quote delimiters for attribute values
- Distinguishes between absent declaration (success) vs malformed declaration (error)

## Simplified Source

```c
static int
parse_xml_decl(const xmlChar *str, size_t *lenp,
               xmlChar **version, xmlChar **encoding, int *standalone)
{
    const xmlChar *p;

    // Initialize libxml and set output defaults
    pg_xml_init_library();
    if (version) *version = NULL;
    if (encoding) *encoding = NULL;
    if (standalone) *standalone = -1;

    p = str;

    // Check for XML declaration start "<?xml"
    if (xmlStrncmp(p, (xmlChar *) "<?xml", 5) != 0)
        goto finished;

    // Ensure it's not a processing instruction like <?xml-stylesheet
    int utf8len = strnlen((const char *) (p + 5), MAX_MULTIBYTE_CHAR_LEN);
    int utf8char = xmlGetUTF8Char(p + 5, &utf8len);
    if (PG_XMLISNAMECHAR(utf8char))
        goto finished;

    p += 5;

    // Parse required version attribute
    SKIP_XML_SPACE(p);
    if (xmlStrncmp(p, (xmlChar *) "version", 7) != 0)
        return XML_ERR_VERSION_MISSING;
    p += 7;
    SKIP_XML_SPACE(p);
    if (*p != '=') return XML_ERR_VERSION_MISSING;
    p++;
    SKIP_XML_SPACE(p);

    // Extract version value in quotes
    if (*p == '\'' || *p == '"') {
        const xmlChar *q = xmlStrchr(p + 1, *p);
        if (!q) return XML_ERR_VERSION_MISSING;
        if (version) *version = xml_pnstrdup(p + 1, q - p - 1);
        p = q + 1;
    } else {
        return XML_ERR_VERSION_MISSING;
    }

    // Parse optional encoding attribute
    const xmlChar *save_p = p;
    SKIP_XML_SPACE(p);
    if (xmlStrncmp(p, (xmlChar *) "encoding", 8) == 0) {
        p += 8;
        SKIP_XML_SPACE(p);
        if (*p != '=') return XML_ERR_MISSING_ENCODING;
        p++;
        SKIP_XML_SPACE(p);

        if (*p == '\'' || *p == '"') {
            const xmlChar *q = xmlStrchr(p + 1, *p);
            if (!q) return XML_ERR_MISSING_ENCODING;
            if (encoding) *encoding = xml_pnstrdup(p + 1, q - p - 1);
            p = q + 1;
        } else {
            return XML_ERR_MISSING_ENCODING;
        }
    } else {
        p = save_p;
    }

    // Parse optional standalone attribute
    save_p = p;
    SKIP_XML_SPACE(p);
    if (xmlStrncmp(p, (xmlChar *) "standalone", 10) == 0) {
        p += 10;
        SKIP_XML_SPACE(p);
        if (*p != '=') return XML_ERR_STANDALONE_VALUE;
        p++;
        SKIP_XML_SPACE(p);

        if (xmlStrncmp(p, (xmlChar *) "'yes'", 5) == 0 ||
            xmlStrncmp(p, (xmlChar *) "\"yes\"", 5) == 0) {
            if (standalone) *standalone = 1;
            p += 5;
        } else if (xmlStrncmp(p, (xmlChar *) "'no'", 4) == 0 ||
                   xmlStrncmp(p, (xmlChar *) "\"no\"", 4) == 0) {
            if (standalone) *standalone = 0;
            p += 4;
        } else {
            return XML_ERR_STANDALONE_VALUE;
        }
    } else {
        p = save_p;
    }

    // Check for proper declaration end "?>"
    SKIP_XML_SPACE(p);
    if (xmlStrncmp(p, (xmlChar *) "?>", 2) != 0)
        return XML_ERR_XMLDECL_NOT_FINISHED;
    p += 2;

finished:
    // Validate ASCII-only content and return length
    size_t len = p - str;
    for (const xmlChar *check = str; check < str + len; check++) {
        if (*check > 127) return XML_ERR_INVALID_CHAR;
    }

    if (lenp) *lenp = len;
    return XML_ERR_OK;
}
```