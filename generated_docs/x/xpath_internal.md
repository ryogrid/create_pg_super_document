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