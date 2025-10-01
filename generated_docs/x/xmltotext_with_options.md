# xmltotext_with_options

## Location
[src/backend/utils/adt/xml.c:656-868](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L656-L868)

## Overview
Converts XML data to text with advanced formatting options including indentation support and proper XML parsing validation, providing comprehensive XML-to-text conversion functionality.

## Definition
text *xmltotext_with_options(xmltype *data, XmlOptionType xmloption_arg, bool indent)

## Detailed Description
The xmltotext_with_options function is a sophisticated XML-to-text converter that supports advanced formatting options. Unlike the simple xmltotext function, this version can parse and validate XML according to specified options (XMLOPTION_DOCUMENT vs XMLOPTION_CONTENT) and can apply indentation formatting to the output.

The function first performs an optimization check: if no special processing is needed (xmloption_arg != XMLOPTION_DOCUMENT and !indent), it returns the input data directly through binary compatibility. Otherwise, it uses libxml2 to parse the XML, validate it according to the specified XML option type, and optionally format it with proper indentation.

When indentation is requested, the function creates a libxml2 save context and carefully handles both document and content-fragment cases. For content fragments, it creates a temporary root node to facilitate proper formatting, then iterates through child nodes while inserting newlines between elements.

## Parameters / Member Variables
- `data`: The input XML data to be converted (xmltype pointer)
- `xmloption_arg`: Specifies how to interpret the XML (XMLOPTION_DOCUMENT or XMLOPTION_CONTENT)
- `indent`: Boolean flag indicating whether to apply indentation formatting to the output

## Dependencies
- Functions called/Symbols referenced:
  - [xml_parse](xml_parse.md) (XML parsing with validation)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (database encoding retrieval)
  - [pg_xml_init](../p/pg_xml_init.md) (XML error context initialization)
  - [parse_xml_decl](../p/parse_xml_decl.md) (XML declaration parsing)
  - [xml_text2xmlChar](xml_text2xmlChar.md) (text to xmlChar conversion)
  - [xml_ereport](xml_ereport.md) (XML-specific error reporting)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md) (text conversion utility)
  - [xmlBuffer_to_xmltype](xmlBuffer_to_xmltype.md) (buffer to XML type conversion)
  - [pg_xml_done](../p/pg_xml_done.md) (XML context cleanup)
- Called from (representative examples):
  - [ExecEvalXmlExpr](../E/ExecEvalXmlExpr.md) (expression evaluation in executor)
  - PG_RETURN_XML_P (via macro usage)

## Notes and Other Information
- Requires libxml2 support (USE_LIBXML compilation flag)
- Implements comprehensive error handling with PG_TRY/PG_CATCH blocks
- Preserves XML declarations when present in input, omits them when absent
- Handles both well-formed documents and content fragments differently
- For content fragments with indentation, creates temporary container nodes
- Automatically strips trailing newlines from document output
- Falls back to NO_XML_SUPPORT() error when libxml2 is not available
- Uses XML save contexts with specific formatting flags (XML_SAVE_FORMAT, XML_SAVE_NO_DECL)
- Manages memory carefully with proper cleanup in exception handlers
- Located in src/backend/utils/adt/xml.c at lines 656-868

## Simplified Source

```c
text *
xmltotext_with_options(xmltype *data, XmlOptionType xmloption_arg, bool indent)
{
    text *result;
    xmlDocPtr doc;
    XmlOptionType parsed_xmloptiontype;
    xmlNodePtr content_nodes;
    xmlBufferPtr buf = NULL;
    xmlSaveCtxtPtr ctxt = NULL;
    PgXmlErrorContext *xmlerrcxt = NULL;

    // Quick path: if no special processing needed, return input directly
    if (xmloption_arg != XMLOPTION_DOCUMENT && !indent) {
        return (text *) data;
    }

    // Parse the XML input according to specified options
    // preserve_whitespace = false when indenting to allow proper formatting
    doc = xml_parse(data, xmloption_arg, !indent, GetDatabaseEncoding(),
                    &parsed_xmloptiontype, &content_nodes, &escontext);

    if (doc == NULL || escontext.error_occurred) {
        if (doc) xmlFreeDoc(doc);
        ereport(ERROR, (errcode(ERRCODE_NOT_AN_XML_DOCUMENT),
                       errmsg("not an XML document")));
    }

    // If no indentation requested, we're done
    if (!indent) {
        xmlFreeDoc(doc);
        return (text *) data;
    }

    // Set up XML processing with error handling
    xmlerrcxt = pg_xml_init(PG_XML_STRICTNESS_ALL);
    buf = xmlBufferCreate();

    if (buf == NULL || xmlerrcxt->err_occurred) {
        xml_ereport(xmlerrcxt, ERROR, ERRCODE_OUT_OF_MEMORY,
                   "could not allocate xmlBuffer");
    }

    // Detect XML declaration and set up save context
    size_t decl_len = 0;
    parse_xml_decl(xml_text2xmlChar(data), &decl_len, NULL, NULL, NULL);

    // Create save context with appropriate flags
    if (decl_len == 0) {
        ctxt = xmlSaveToBuffer(buf, NULL, XML_SAVE_NO_DECL | XML_SAVE_FORMAT);
    } else {
        ctxt = xmlSaveToBuffer(buf, NULL, XML_SAVE_FORMAT);
    }

    if (ctxt == NULL || xmlerrcxt->err_occurred) {
        xml_ereport(xmlerrcxt, ERROR, ERRCODE_OUT_OF_MEMORY,
                   "could not allocate xmlSaveCtxt");
    }

    // Handle document vs content fragment differently
    if (parsed_xmloptiontype == XMLOPTION_DOCUMENT) {
        // Simple case: save complete document
        if (xmlSaveDoc(ctxt, doc) == -1 || xmlerrcxt->err_occurred) {
            xml_ereport(xmlerrcxt, ERROR, ERRCODE_INTERNAL_ERROR,
                       "could not save document to xmlBuffer");
        }
    } else if (content_nodes != NULL) {
        // Complex case: handle content fragments with proper formatting

        // Create temporary root container for content nodes
        xmlNodePtr root = xmlNewNode(NULL, (const xmlChar *) "content-root");
        xmlNodePtr oldroot = xmlDocSetRootElement(doc, root);
        if (oldroot != NULL) xmlFreeNode(oldroot);

        xmlAddChildList(root, content_nodes);

        // Create newline node for formatting
        xmlNodePtr newline = xmlNewDocText(NULL, (const xmlChar *) "\n");

        // Iterate through child nodes, adding newlines between elements
        for (xmlNodePtr node = root->children; node; node = node->next) {
            // Insert newlines between non-text nodes
            if (node->type != XML_TEXT_NODE && node->prev != NULL) {
                xmlSaveTree(ctxt, newline);
            }
            xmlSaveTree(ctxt, node);
        }

        xmlFreeNode(newline);
    }

    xmlSaveClose(ctxt);

    // Clean up trailing newlines for documents
    if (xmloption_arg == XMLOPTION_DOCUMENT) {
        const char *str = (const char *) xmlBufferContent(buf);
        int len = xmlBufferLength(buf);

        while (len > 0 && (str[len - 1] == '\n' || str[len - 1] == '\r')) {
            len--;
        }

        result = cstring_to_text_with_len(str, len);
    } else {
        result = (text *) xmlBuffer_to_xmltype(buf);
    }

    // Cleanup
    xmlBufferFree(buf);
    xmlFreeDoc(doc);
    pg_xml_done(xmlerrcxt, false);

    return result;
}
```

This simplified version preserves the essential algorithm while removing:
- Complex PG_TRY/PG_CATCH exception handling blocks
- Detailed error state management and volatile declarations
- Platform-specific preprocessing directives (#ifdef USE_LIBXML)
- Redundant error checking and recovery logic
- Detailed memory leak prevention code in error paths

The core logic flow remains intact: quick path optimization, XML parsing with validation, conditional indentation processing, different handling for documents vs content fragments, and proper resource cleanup.