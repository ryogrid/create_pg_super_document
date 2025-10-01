# xml_errorHandler

## Location
[src/backend/utils/adt/xml.c:2088-2275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2088-L2275)

## Overview
The central error handler callback function for libxml2 that captures, processes, and formats XML parsing errors according to PostgreSQL's error reporting standards.

## Definition

```c
struct.
	 *
	 * We force a backend exit if this check fails because longjmp'ing out of
	 * libxml would likely render it unsafe to use further.
	 */
	if (xmlerrcxt->magic != ERRCXT_MAGIC)
		elog(FATAL, "xml_errorHandler called with invalid PgXmlErrorContext");
```
## Detailed Description
xml_errorHandler is a comprehensive error handler that serves as the bridge between libxml2's error system and PostgreSQL's error reporting infrastructure. It processes various types of XML errors (parser errors, namespace errors, etc.), applies version-specific compatibility fixes, formats detailed error messages with context information, and either stores them for later reporting or reports them immediately based on severity.

The handler implements sophisticated logic to handle differences between libxml2 versions, suppress certain non-critical warnings, and provide rich context information including line numbers, element names, and file context when available.

## Parameters / Member Variables
- `data`: Pointer to PgXmlErrorContext structure (passed as void* for libxml2 compatibility)
- `error`: PgXmlErrorPtr containing detailed error information from libxml2

## Dependencies
- Functions called/Symbols referenced:
  - PgXmlErrorPtr, PgXmlErrorContext structures
  - ERRCXT_MAGIC (validation constant)
  - PG_XML_STRICTNESS_WELLFORMED, PG_XML_STRICTNESS_LEGACY (strictness levels)
  - [makeStringInfo](../m/makeStringInfo.md), appendStringInfo, appendStringInfoLineSeparator
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md), destroyStringInfo, chopStringInfoNewlines
  - xmlParserPrintFileContext, xmlSetGenericErrorFunc (libxml2 functions)
  - ereport, errmsg_internal, WARNING, NOTICE (PostgreSQL error system)
- Called from (representative examples):
  - Registered as callback in pg_xml_init
  - Used by XmlTable functions
  - Referenced in PgXmlErrorContext structure

## Notes and Other Information
- Implements cross-version compatibility for libxml2 behavior differences
- Handles special cases like XML_ERR_NOT_WELL_BALANCED suppression to avoid redundant errors
- Suppresses XML_WAR_UNDECLARED_ENTITY warnings to avoid DTD-related issues
- Uses sophisticated context formatting by temporarily hijacking libxml2's generic error handler
- Supports legacy mode for backward compatibility with the deprecated xml2 contrib module
- Errors are buffered rather than immediately reported to avoid leaving libxml2 in inconsistent state
- Warnings and notices are reported immediately since they don't cause longjmp() out of libxml2
- Forces backend exit (FATAL) if called with invalid context to prevent undefined behavior

## Simplified Source

```c
static void
xml_errorHandler(void *data, PgXmlErrorPtr error)
{
    PgXmlErrorContext *xmlerrcxt = (PgXmlErrorContext *) data;
    xmlParserCtxtPtr ctxt = (xmlParserCtxtPtr) error->ctxt;
    xmlParserInputPtr input = (ctxt != NULL) ? ctxt->input : NULL;
    xmlNodePtr node = error->node;
    const xmlChar *name = (node != NULL && node->type == XML_ELEMENT_NODE) ? node->name : NULL;
    int domain = error->domain;
    int level = error->level;
    StringInfo errorBuf;

    // Validate context structure
    if (xmlerrcxt->magic != ERRCXT_MAGIC)
        elog(FATAL, "xml_errorHandler called with invalid PgXmlErrorContext");

    // Handle libxml version compatibility issues
    switch (error->code)
    {
        case XML_WAR_NS_URI:
            level = XML_ERR_ERROR;
            domain = XML_FROM_NAMESPACE;
            break;
        case XML_ERR_NS_DECL_ERROR:
        case XML_WAR_NS_URI_RELATIVE:
        case XML_WAR_NS_COLUMN:
        case XML_NS_ERR_XML_NAMESPACE:
        case XML_NS_ERR_UNDEFINED_NAMESPACE:
        case XML_NS_ERR_QNAME:
        case XML_NS_ERR_ATTRIBUTE_REDEFINED:
        case XML_NS_ERR_EMPTY:
            domain = XML_FROM_NAMESPACE;
            break;
    }

    // Filter errors based on domain and strictness
    switch (domain)
    {
        case XML_FROM_PARSER:
            // Suppress redundant NOT_WELL_BALANCED errors
            if (error->code == XML_ERR_NOT_WELL_BALANCED && xmlerrcxt->err_occurred)
                return;
            // fall through
        case XML_FROM_NONE:
        case XML_FROM_MEMORY:
        case XML_FROM_IO:
            // Suppress undeclared entity warnings
            if (error->code == XML_WAR_UNDECLARED_ENTITY)
                return;
            break;
        default:
            // Ignore non-parser errors in well-formedness mode
            if (xmlerrcxt->strictness == PG_XML_STRICTNESS_WELLFORMED)
                return;
            break;
    }

    // Build error message with context
    errorBuf = makeStringInfo();

    if (error->line > 0)
        appendStringInfo(errorBuf, "line %d: ", error->line);
    if (name != NULL)
        appendStringInfo(errorBuf, "element %s: ", name);
    if (error->message != NULL)
        appendStringInfoString(errorBuf, error->message);
    else
        appendStringInfoString(errorBuf, "(no message provided)");

    // Add file context using libxml's formatter
    if (input != NULL)
    {
        xmlGenericErrorFunc errFuncSaved = xmlGenericError;
        void *errCtxSaved = xmlGenericErrorContext;

        xmlSetGenericErrorFunc((void *) errorBuf, (xmlGenericErrorFunc) appendStringInfo);
        appendStringInfoLineSeparator(errorBuf);
        xmlParserPrintFileContext(input);
        xmlSetGenericErrorFunc(errCtxSaved, errFuncSaved);
    }

    chopStringInfoNewlines(errorBuf);

    // Handle legacy mode
    if (xmlerrcxt->strictness == PG_XML_STRICTNESS_LEGACY)
    {
        appendStringInfoLineSeparator(&xmlerrcxt->err_buf);
        appendBinaryStringInfo(&xmlerrcxt->err_buf, errorBuf->data, errorBuf->len);
        destroyStringInfo(errorBuf);
        return;
    }

    // Report based on severity level
    if (level >= XML_ERR_ERROR)
    {
        // Buffer errors for later reporting
        appendStringInfoLineSeparator(&xmlerrcxt->err_buf);
        appendBinaryStringInfo(&xmlerrcxt->err_buf, errorBuf->data, errorBuf->len);
        xmlerrcxt->err_occurred = true;
    }
    else if (level >= XML_ERR_WARNING)
    {
        ereport(WARNING, (errmsg_internal("%s", errorBuf->data)));
    }
    else
    {
        ereport(NOTICE, (errmsg_internal("%s", errorBuf->data)));
    }

    destroyStringInfo(errorBuf);
}
```