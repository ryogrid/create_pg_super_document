# xpath_internal

## Location
[src/backend/utils/adt/xml.c:4324-4520](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L4324-L4520)

## Overview
Core implementation function for PostgreSQL's xpath() and xmlexists() operations that evaluates XPath expressions against XML documents using libxml2.

## Definition
```c
static void xpath_internal(text *xpath_expr_text, xmltype *data, ArrayType *namespaces,
                          int *res_nitems, ArrayBuildState *astate)
```

## Detailed Description
This function serves as the common implementation core for PostgreSQL's XPath functionality. It parses XML documents, sets up XPath evaluation contexts with optional namespace mappings, compiles and evaluates XPath expressions, and converts results to PostgreSQL arrays. The function handles UTF-8 encoding considerations by skipping XML declarations in UTF-8 databases to avoid encoding conflicts. It supports namespace registration through a 2-dimensional text array and provides comprehensive error handling using PostgreSQL's exception system. The function can operate in different modes: returning both result count and array data, or just one of them.

## Parameters / Member Variables
- `xpath_expr_text`: The XPath expression to evaluate as PostgreSQL text
- `data`: The XML document data as PostgreSQL xmltype
- `namespaces`: Optional 2D array of namespace name/URI pairs (can be NULL)
- `res_nitems`: Pointer to store the number of result items (can be NULL)
- `astate`: ArrayBuildState for building result array (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [xml_xpathobjtoxmlarray](xml_xpathobjtoxmlarray.md)
  - [pg_xml_init](../p/pg_xml_init.md), pg_xml_done
  - [xml_ereport](xml_ereport.md)
  - [parse_xml_decl](../p/parse_xml_decl.md)
  - [pg_xmlCharStrndup](../p/pg_xmlCharStrndup.md)
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - xmlNewParserCtxt, xmlCtxtReadMemory, xmlXPathNewContext, xmlXPathCtxtCompile, xmlXPathCompiledEval (libxml2)
- Called from (representative examples):
  - [xpath](xpath.md)
  - [xmlexists](xmlexists.md)
  - [xpath_exists](xpath_exists.md)

## Notes and Other Information
- Static function providing shared implementation for multiple XPath-related SQL functions
- Includes workaround for libxml2 recursion vulnerability (uses xmlXPathCtxtCompile instead of xmlXPathCompile)
- Handles UTF-8 database encoding specially by skipping XML declarations to avoid encoding conflicts
- Supports both counted and uncounted result modes for flexibility
- Comprehensive memory management with proper cleanup in both normal and error paths
- Namespace array format: [[name1, uri1], [name2, uri2], ...] as 2D text array
- Part of PostgreSQL's SQL/XML standard compliance implementation
- Requires valid XML documents (fragments without context nodes not well supported)

## Simplified Source

```c
static void xpath_internal(text *xpath_expr_text, xmltype *data, ArrayType *namespaces,
                          int *res_nitems, ArrayBuildState *astate)
{
    PgXmlErrorContext *xmlerrcxt;
    volatile xmlParserCtxtPtr ctxt = NULL;
    volatile xmlDocPtr doc = NULL;
    volatile xmlXPathContextPtr xpathctx = NULL;
    volatile xmlXPathCompExprPtr xpathcomp = NULL;
    volatile xmlXPathObjectPtr xpathobj = NULL;

    char *datastr;
    int32 len, xpath_len;
    xmlChar *string, *xpath_expr;
    size_t xmldecl_len = 0;

    // Process namespace mappings (must be 2D array: [[name, uri], ...])
    int ndim = namespaces ? ARR_NDIM(namespaces) : 0;
    Datum *ns_names_uris = NULL;
    bool *ns_names_uris_nulls = NULL;
    int ns_count = 0;

    if (ndim != 0) {
        int *dims = ARR_DIMS(namespaces);
        if (ndim != 2 || dims[1] != 2)
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("invalid array for XML namespace mapping")));

        deconstruct_array_builtin(namespaces, TEXTOID, &ns_names_uris,
                                 &ns_names_uris_nulls, &ns_count);
        ns_count /= 2;  // count pairs only
    }

    // Extract XML data and XPath expression
    datastr = VARDATA(data);
    len = VARSIZE(data) - VARHDRSZ;
    xpath_len = VARSIZE_ANY_EXHDR(xpath_expr_text);

    if (xpath_len == 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("empty XPath expression")));

    string = pg_xmlCharStrndup(datastr, len);
    xpath_expr = pg_xmlCharStrndup(VARDATA_ANY(xpath_expr_text), xpath_len);

    // Skip XML declaration in UTF8 databases to avoid encoding conflicts
    if (GetDatabaseEncoding() == PG_UTF8)
        parse_xml_decl(string, &xmldecl_len, NULL, NULL, NULL);

    xmlerrcxt = pg_xml_init(PG_XML_STRICTNESS_ALL);

    PG_TRY();
    {
        // Initialize XML parser and parse document
        xmlInitParser();
        ctxt = xmlNewParserCtxt();
        if (ctxt == NULL || xmlerrcxt->err_occurred)
            xml_ereport(xmlerrcxt, ERROR, ERRCODE_OUT_OF_MEMORY,
                       "could not allocate parser context");

        doc = xmlCtxtReadMemory(ctxt, (char *) string + xmldecl_len,
                               len - xmldecl_len, NULL, NULL, 0);
        if (doc == NULL || xmlerrcxt->err_occurred)
            xml_ereport(xmlerrcxt, ERROR, ERRCODE_INVALID_XML_DOCUMENT,
                       "could not parse XML document");

        // Create XPath context and register namespaces
        xpathctx = xmlXPathNewContext(doc);
        if (xpathctx == NULL || xmlerrcxt->err_occurred)
            xml_ereport(xmlerrcxt, ERROR, ERRCODE_OUT_OF_MEMORY,
                       "could not allocate XPath context");
        xpathctx->node = (xmlNodePtr) doc;

        // Register namespaces if provided
        for (int i = 0; i < ns_count; i++) {
            if (ns_names_uris_nulls[i * 2] || ns_names_uris_nulls[i * 2 + 1])
                ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("neither namespace name nor URI may be null")));

            char *ns_name = TextDatumGetCString(ns_names_uris[i * 2]);
            char *ns_uri = TextDatumGetCString(ns_names_uris[i * 2 + 1]);

            if (xmlXPathRegisterNs(xpathctx, (xmlChar *) ns_name, (xmlChar *) ns_uri) != 0)
                ereport(ERROR, (errmsg("could not register XML namespace")));
        }

        // Compile and evaluate XPath expression
        xpathcomp = xmlXPathCtxtCompile(xpathctx, xpath_expr);
        if (xpathcomp == NULL || xmlerrcxt->err_occurred)
            xml_ereport(xmlerrcxt, ERROR, ERRCODE_INTERNAL_ERROR,
                       "invalid XPath expression");

        xpathobj = xmlXPathCompiledEval(xpathcomp, xpathctx);
        if (xpathobj == NULL || xmlerrcxt->err_occurred)
            xml_ereport(xmlerrcxt, ERROR, ERRCODE_INTERNAL_ERROR,
                       "could not create XPath object");

        // Extract results
        if (res_nitems != NULL)
            *res_nitems = xml_xpathobjtoxmlarray(xpathobj, astate, xmlerrcxt);
        else
            xml_xpathobjtoxmlarray(xpathobj, astate, xmlerrcxt);
    }
    PG_CATCH();
    {
        // Cleanup on error
        if (xpathobj) xmlXPathFreeObject(xpathobj);
        if (xpathcomp) xmlXPathFreeCompExpr(xpathcomp);
        if (xpathctx) xmlXPathFreeContext(xpathctx);
        if (doc) xmlFreeDoc(doc);
        if (ctxt) xmlFreeParserCtxt(ctxt);
        pg_xml_done(xmlerrcxt, true);
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Normal cleanup
    xmlXPathFreeObject(xpathobj);
    xmlXPathFreeCompExpr(xpathcomp);
    xmlXPathFreeContext(xpathctx);
    xmlFreeDoc(doc);
    xmlFreeParserCtxt(ctxt);
    pg_xml_done(xmlerrcxt, false);
}
```