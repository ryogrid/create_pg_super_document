# xmlconcat

## Location
[src/backend/utils/adt/xml.c:553-618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L553-L618)

## Overview
Concatenates multiple XML values into a single XML document, handling XML declaration merging and validation.

## Definition
```c
xmltype *xmlconcat(List *args)
```

## Detailed Description
The xmlconcat function takes a list of XML values and concatenates them into a single XML document. It performs sophisticated handling of XML declarations by:

1. Parsing each input XML value to extract version and standalone attributes from XML declarations
2. Determining a global version and standalone value based on the inputs
3. Merging the content portions (excluding declarations) of all input XML values
4. Prepending a unified XML declaration if necessary

The function ensures that the resulting XML document has consistent declaration attributes. If all input documents have the same version, that version is used globally. If there are conflicts or missing versions, no version is included in the final declaration.

For standalone attributes:
- If any input has standalone="no", the result uses standalone="no"
- If any input has an unspecified standalone, the result has unspecified standalone
- Otherwise, standalone="yes" is used

## Parameters / Member Variables
- `args`: List of XML values to concatenate

## Dependencies
- Functions called/Symbols referenced:
  - [xmltype](xmltype.md)
  - [DatumGetXmlP](../D/DatumGetXmlP.md)
  - VARSIZE
  - [text_to_cstring](../t/text_to_cstring.md)
  - [parse_xml_decl](../p/parse_xml_decl.md)
  - [print_xml_decl](../p/print_xml_decl.md)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
  - [stringinfo_to_xmltype](../s/stringinfo_to_xmltype.md)
  - NO_XML_SUPPORT (fallback when libxml not available)
- Called from (representative examples):
  - [ExecEvalXmlExpr](../E/ExecEvalXmlExpr.md) (src/backend/executor/execExprInterp.c:3910)
  - [xmlconcat2](xmlconcat2.md) (src/backend/utils/adt/xml.c:631)
  - PG_RETURN_XML_P (src/include/utils/xml.h:72)

## Notes and Other Information
- Function is only available when PostgreSQL is compiled with libxml support (`USE_LIBXML`)
- TODO comment indicates that merging notations and unparsed entities is not implemented
- Handles complex XML declaration merging logic for proper XML document formation
- Memory management includes proper cleanup with pfree for temporary strings
- Used internally by PostgreSQL's XML expression evaluation system
- Returns NULL when libxml support is not available