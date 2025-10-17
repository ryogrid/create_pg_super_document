# xmltext

## Location
[src/backend/utils/adt/xml.c:527-552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L527-L552)

## Overview
Converts text input into properly escaped XML text content by encoding special characters that have meaning in XML.

## Definition
```c
Datum xmltext(PG_FUNCTION_ARGS)
```

## Detailed Description
The xmltext function creates XML text content from input text by properly escaping special characters that have semantic meaning in XML (such as <, >, &, etc.). This ensures that the text can be safely included as content within XML elements without being interpreted as XML markup.

The function uses libxml's xmlEncodeSpecialChars function to perform the character escaping, which handles the conversion of characters like:
- & to &amp;
- < to &lt;
- > to &gt;
- " to &quot;
- ' to &#39;

The resulting escaped text is returned as PostgreSQL's XML type.

## Parameters / Member Variables
- Input text parameter (accessed via `PG_GETARG_TEXT_PP(0)`): The text content to be XML-escaped

## Dependencies
- Functions called/Symbols referenced:
  - [xml_text2xmlChar](xml_text2xmlChar.md)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md)
  - PG_RETURN_XML_P
  - NO_XML_SUPPORT (fallback when libxml not available)
  - [xmltype](xmltype.md)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- Function is only available when PostgreSQL is compiled with libxml support (`USE_LIBXML`)
- Uses libxml2's xmlEncodeSpecialChars for proper XML character escaping
- Essential for safely including arbitrary text content within XML documents
- Memory management handled with xmlFree to avoid leaks
- Returns XML type compatible with PostgreSQL's XML handling system

## Simplified Source

```c
Datum xmltext(PG_FUNCTION_ARGS) {
#ifdef USE_LIBXML
    text *arg = PG_GETARG_TEXT_PP(0);

    // Convert to XML encoding and escape special characters
    xmlChar *xmlbuf = xmlEncodeSpecialChars(NULL, xml_text2xmlChar(arg));
    Assert(xmlbuf);

    // Convert back to PostgreSQL text with proper length
    text *result = cstring_to_text_with_len((const char *) xmlbuf, xmlStrlen(xmlbuf));
    xmlFree(xmlbuf);

    PG_RETURN_XML_P(result);
#else
    NO_XML_SUPPORT();
    return 0;
#endif
}
```