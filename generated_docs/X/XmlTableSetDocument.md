# XmlTableSetDocument

## Location
[src/backend/utils/adt/xml.c:4732-4788](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L4732-L4788)

## Overview
Parses and installs an XML document into the XmlTable processing context, setting up the document and XPath context for subsequent XML table operations.

## Definition
static void XmlTableSetDocument(TableFuncScanState *state, Datum value)

## Detailed Description
This function takes an XML document as input and prepares it for XmlTable processing by parsing it using libxml2 and creating an XPath evaluation context. The function converts the input XML datum to a string representation, parses it into a libxml2 document structure, and creates an XPath context that will be used for evaluating XPath expressions in column definitions.

The function employs PostgreSQL's exception handling mechanism to ensure proper cleanup of allocated libxml2 resources if parsing fails. It uses the xml_out_internal function to convert the XML to a string format suitable for libxml2 parsing, removing encoding properties that might interfere with parsing.

## Parameters / Member Variables
- state: TableFuncScanState* - The table function scan state containing the XmlTable context
- value: Datum - The XML document to be installed and parsed

## Dependencies
- Functions called/Symbols referenced:
  - [GetXmlTableBuilderPrivateData](../G/GetXmlTableBuilderPrivateData.md) (retrieves XmlTable context)
  - [DatumGetXmlP](../D/DatumGetXmlP.md) (converts Datum to xmltype)
  - [xml_out_internal](../x/xml_out_internal.md) (converts XML to string)
  - [pg_xmlCharStrndup](../p/pg_xmlCharStrndup.md) (creates libxml2 string)
  - xmlCtxtReadMemory (libxml2 document parsing)
  - xmlXPathNewContext (XPath context creation)
  - [xml_ereport](../x/xml_ereport.md) (XML error reporting)
  - xmlXPathFreeContext, xmlFreeDoc (cleanup functions)
  - PG_TRY/PG_CATCH/PG_END_TRY (exception handling)
- Called from (representative examples):
  - No direct callers found (likely called via table function interface)

## Dependencies
- Functions called/Symbols referenced:
  - [GetXmlTableBuilderPrivateData](../G/GetXmlTableBuilderPrivateData.md)
  - [DatumGetXmlP](../D/DatumGetXmlP.md)
  - [xml_out_internal](../x/xml_out_internal.md)
  - [pg_xmlCharStrndup](../p/pg_xmlCharStrndup.md)
  - xmlCtxtReadMemory
  - xmlXPathNewContext
  - [xml_ereport](../x/xml_ereport.md)
- Called from (representative examples):
  - No direct callers found (likely called via table function interface)

## Notes and Other Information
- Only available when PostgreSQL is compiled with libxml2 support (USE_LIBXML)
- Uses xml_out_internal to strip encoding information before parsing
- Creates both document and XPath contexts for subsequent processing
- Employs robust error handling with automatic resource cleanup
- Located in src/backend/utils/adt/xml.c:4732-4788
- Sets the document root as the initial XPath context node
- Critical step in XmlTable processing pipeline

## Simplified Source

```c
static void
XmlTableSetDocument(TableFuncScanState *state, Datum value)
{
#ifdef USE_LIBXML
    XmlTableBuilderData *xtCxt = GetXmlTableBuilderPrivateData(state, "XmlTableSetDocument");
    xmltype *xmlval = DatumGetXmlP(value);

    // Convert XML to string format suitable for parsing
    char *str = xml_out_internal(xmlval, 0);
    int length = strlen(str);
    xmlChar *xstr = pg_xmlCharStrndup(str, length);

    volatile xmlDocPtr doc = NULL;
    volatile xmlXPathContextPtr xpathcxt = NULL;

    // Parse XML document and create XPath context with cleanup on failure
    PG_TRY();
    {
        // Parse the XML document
        doc = xmlCtxtReadMemory(xtCxt->ctxt, (char *) xstr, length, NULL, NULL, 0);
        if (doc == NULL || xtCxt->xmlerrcxt->err_occurred)
            xml_ereport(xtCxt->xmlerrcxt, ERROR, ERRCODE_INVALID_XML_DOCUMENT,
                        "could not parse XML document");

        // Create XPath evaluation context
        xpathcxt = xmlXPathNewContext(doc);
        if (xpathcxt == NULL || xtCxt->xmlerrcxt->err_occurred)
            xml_ereport(xtCxt->xmlerrcxt, ERROR, ERRCODE_OUT_OF_MEMORY,
                        "could not allocate XPath context");

        xpathcxt->node = (xmlNodePtr) doc;
    }
    PG_CATCH();
    {
        // Clean up on error
        if (xpathcxt != NULL)
            xmlXPathFreeContext(xpathcxt);
        if (doc != NULL)
            xmlFreeDoc(doc);
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Store the parsed document and context for later use
    xtCxt->doc = doc;
    xtCxt->xpathcxt = xpathcxt;
#else
    NO_XML_SUPPORT();
#endif
}
```