# xml_parse

## Location
[src/backend/utils/adt/xml.c:1748-1932](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1748-L1932)

## Overview
The core XML parsing function that converts text data to libxml2's internal xmlDoc representation, supporting both DOCUMENT and CONTENT parsing modes with comprehensive error handling.

## Definition

```c
static xmlDocPtr
xml_parse(text *data, XmlOptionType xmloption_arg,
		  bool preserve_whitespace, int encoding,
		  XmlOptionType *parsed_xmloptiontype, xmlNodePtr *parsed_nodes,
		  Node *escontext)
```
## Detailed Description
This function serves as PostgreSQL's central XML parsing engine, bridging between PostgreSQL's text representation and libxml2's DOM representation. It implements sophisticated parsing logic to handle both SQL/XML DOCUMENT and CONTENT modes while providing robust error handling and resource management.

Key parsing behaviors:
- **Encoding handling**: Automatically converts input from specified encoding to UTF-8 for libxml2
- **Mode determination**: Intelligently switches between DOCUMENT and CONTENT parsing based on input characteristics
- **DTD support**: Implements SQL/XML:2006+ compliant parsing by detecting DTDs in CONTENT mode
- **Error management**: Supports both hard errors (ereport) and soft errors (ErrorSaveContext)
- **Resource safety**: Uses PG_TRY/PG_CATCH blocks for proper cleanup

The function first extracts and validates any XML declaration, then determines the appropriate parsing mode. For DOCUMENT mode, it uses xmlCtxtReadDoc with full validation. For CONTENT mode, it uses xmlParseBalancedChunkMemory to parse XML fragments.

## Parameters / Member Variables
- `*data`: Source text data to parse (must not be toasted)
- `xmloption_arg`: Requested parsing mode (XMLOPTION_DOCUMENT or XMLOPTION_CONTENT)
- `preserve_whitespace`: Whether to preserve whitespace nodes in parsed result
- `encoding`: Character encoding of the input data
- `*parsed_xmloptiontype`: Output parameter for actual parsing mode used (can be NULL)
- `*parsed_nodes`: Output parameter for parsed node list in CONTENT mode (can be NULL)
- `*escontext`: Error context for soft error handling (can be NULL for hard errors)
## Dependencies
- Functions called/Symbols referenced:
  - [xml_text2xmlChar](xml_text2xmlChar.md) (convert PostgreSQL text to xmlChar string)
  - [pg_do_encoding_conversion](../p/pg_do_encoding_conversion.md) (convert text encoding to UTF-8)
  - [pg_xml_init](../p/pg_xml_init.md) (initialize XML error context and libxml2)
  - [parse_xml_decl](../p/parse_xml_decl.md) (parse and validate XML declaration)
  - [xml_doctype_in_content](xml_doctype_in_content.md) (detect DTD in CONTENT mode)
  - xmlNewParserCtxt, xmlCtxtReadDoc (libxml2 document parsing)
  - xmlParseBalancedChunkMemory (libxml2 fragment parsing)
  - [xml_ereport](xml_ereport.md), xml_errsave, errsave (PostgreSQL error reporting)
  - [pg_xml_done](../p/pg_xml_done.md) (cleanup XML error context)
- Called from (representative examples):
  - [xml_in](xml_in.md) (XML input function)
  - [xml_recv](xml_recv.md) (XML binary input function)
  - [xmlparse](xmlparse.md) (SQL/XML XMLPARSE function)
  - [xml_is_document](xml_is_document.md) (XML validation function)
  - [wellformed_xml](../w/wellformed_xml.md) (XML well-formedness checking)

## Notes and Other Information
- Returns xmlDocPtr on success, NULL on soft error (caller must check SOFT_ERROR_OCCURRED())
- Function is static (internal to xml.c file)
- Caller is responsible for calling xmlFreeDoc() on returned document to prevent memory leaks
- Automatically promotes CONTENT to DOCUMENT mode when DTD is detected (SQL/XML:2006+ compliance)
- Supports DTD attribute defaults in DOCUMENT mode per SQL/XML:2008 standard
- Uses XML_PARSE_NOBLANKS option when whitespace preservation is disabled
- Handles empty XML content gracefully in CONTENT mode
- Comprehensive resource cleanup in exception handlers prevents memory leaks
- Sets document encoding to UTF-8 and preserves standalone attribute from XML declaration

## Simplified Source

```c
static xmlDocPtr
xml_parse(text *data, XmlOptionType xmloption_arg,
          bool preserve_whitespace, int encoding,
          XmlOptionType *parsed_xmloptiontype, xmlNodePtr *parsed_nodes,
          Node *escontext)
{
    // Convert input data to UTF-8 string
    int32 len = VARSIZE_ANY_EXHDR(data);
    xmlChar *string = xml_text2xmlChar(data);
    xmlChar *utf8string = pg_do_encoding_conversion(string, len, encoding, PG_UTF8);

    // Initialize XML parsing context
    PgXmlErrorContext *xmlerrcxt = pg_xml_init(PG_XML_STRICTNESS_WELLFORMED);
    volatile xmlDocPtr doc = NULL;
    volatile xmlParserCtxtPtr ctxt = NULL;
    volatile int save_keep_blanks = -1;

    PG_TRY();
    {
        xmlInitParser();
        bool parse_as_document = false;

        // Determine parsing mode (DOCUMENT vs CONTENT)
        if (xmloption_arg == XMLOPTION_DOCUMENT) {
            parse_as_document = true;
        } else {
            // Parse XML declaration and check for DOCTYPE
            size_t count = 0;
            xmlChar *version = NULL;
            int standalone = 0;

            int res_code = parse_xml_decl(utf8string, &count, &version, NULL, &standalone);
            if (res_code != 0) {
                errsave(escontext, errcode(ERRCODE_INVALID_XML_CONTENT),
                        errmsg_internal("invalid XML content: invalid XML declaration"));
                goto fail;
            }

            if (xml_doctype_in_content(utf8string + count))
                parse_as_document = true;
        }

        // Set output parameters
        if (parsed_xmloptiontype != NULL)
            *parsed_xmloptiontype = parse_as_document ? XMLOPTION_DOCUMENT : XMLOPTION_CONTENT;
        if (parsed_nodes != NULL)
            *parsed_nodes = NULL;

        if (parse_as_document) {
            // Parse as complete XML document
            ctxt = xmlNewParserCtxt();
            if (ctxt == NULL || xmlerrcxt->err_occurred)
                xml_ereport(xmlerrcxt, ERROR, ERRCODE_OUT_OF_MEMORY,
                           "could not allocate parser context");

            int options = XML_PARSE_NOENT | XML_PARSE_DTDATTR
                         | (preserve_whitespace ? 0 : XML_PARSE_NOBLANKS);

            doc = xmlCtxtReadDoc(ctxt, utf8string, NULL, "UTF-8", options);

            if (doc == NULL || xmlerrcxt->err_occurred) {
                if (xmloption_arg == XMLOPTION_DOCUMENT)
                    xml_errsave(escontext, xmlerrcxt, ERRCODE_INVALID_XML_DOCUMENT,
                               "invalid XML document");
                else
                    xml_errsave(escontext, xmlerrcxt, ERRCODE_INVALID_XML_CONTENT,
                               "invalid XML content");
                goto fail;
            }
        } else {
            // Parse as XML content fragment
            doc = xmlNewDoc(version);
            if (doc == NULL || xmlerrcxt->err_occurred)
                xml_ereport(xmlerrcxt, ERROR, ERRCODE_OUT_OF_MEMORY,
                           "could not allocate XML document");

            doc->encoding = xmlStrdup((const xmlChar *) "UTF-8");
            doc->standalone = standalone;

            save_keep_blanks = xmlKeepBlanksDefault(preserve_whitespace ? 1 : 0);

            // Parse content if not empty
            if (*(utf8string + count)) {
                int res_code = xmlParseBalancedChunkMemory(doc, NULL, NULL, 0,
                                                          utf8string + count,
                                                          parsed_nodes);
                if (res_code != 0 || xmlerrcxt->err_occurred) {
                    xml_errsave(escontext, xmlerrcxt, ERRCODE_INVALID_XML_CONTENT,
                               "invalid XML content");
                    goto fail;
                }
            }
        }

fail:
        ;
    }
    PG_CATCH();
    {
        // Cleanup on exception
        if (save_keep_blanks != -1)
            xmlKeepBlanksDefault(save_keep_blanks);
        if (doc != NULL)
            xmlFreeDoc(doc);
        if (ctxt != NULL)
            xmlFreeParserCtxt(ctxt);
        pg_xml_done(xmlerrcxt, true);
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Normal cleanup
    if (save_keep_blanks != -1)
        xmlKeepBlanksDefault(save_keep_blanks);
    if (ctxt != NULL)
        xmlFreeParserCtxt(ctxt);
    pg_xml_done(xmlerrcxt, false);

    return doc;
}
```