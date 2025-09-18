# DatumGetXmlP

## Location
[src/include/utils/xml.h:51-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/xml.h#L51-L56)

## Overview
DatumGetXmlP is an inline function that converts a PostgreSQL Datum value to an xmltype pointer, handling TOAST decompression if necessary.

## Definition


## Detailed Description
This function provides a convenient way to extract an xmltype pointer from a Datum value. It uses PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) mechanism to handle potentially compressed or out-of-line XML data. The function automatically decompresses TOASTed XML values and returns a pointer to the xmltype structure that can be directly used by XML processing functions.

## Parameters / Member Variables
- `X`: A Datum value that contains an XML value, potentially in TOASTed form

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM
  - [xmltype](../x/xmltype.md)
- Called from (representative examples):
  - [ExecEvalXmlExpr](../E/ExecEvalXmlExpr.md) (src/backend/executor/execExprInterp.c:4036, 4066, 4086)
  - [xmlconcat](../x/xmlconcat.md) (src/backend/utils/adt/xml.c:565)
  - [XmlTableSetDocument](../X/XmlTableSetDocument.md) (src/backend/utils/adt/xml.c:4736)
  - PG_GETARG_XML_P (src/include/utils/xml.h:62)

## Notes and Other Information
- This is a static inline function defined in src/include/utils/xml.h
- The function handles TOAST decompression transparently, ensuring that the returned xmltype pointer points to accessible XML data
- Part of PostgreSQL's XML data type infrastructure for efficient handling of variable-length XML values
- Always use this function rather than direct casting when converting Datum to xmltype* to ensure proper TOAST handling