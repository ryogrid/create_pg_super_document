# xmlparse

## Location
[src/backend/utils/adt/xml.c:993-1010](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L993-L1010)

## Overview
Parses and validates text data as XML according to specified XML option types, providing input validation for XML data in PostgreSQL.

## Definition
xmltype *xmlparse(text *data, XmlOptionType xmloption_arg, bool preserve_whitespace)

## Detailed Description
The xmlparse function validates text input as well-formed XML by parsing it using the internal xml_parse function. It serves as a validation gateway that ensures the input conforms to the specified XML option type (either XMLOPTION_DOCUMENT or XMLOPTION_CONTENT) before accepting it as valid XML data.

The function performs validation only and does not modify the input data. After successful parsing and validation through xml_parse, it immediately frees the parsed document structure and returns the original input data cast to xmltype. This approach leverages the binary compatibility between text and xmltype while ensuring the data is valid XML.

If parsing fails, the xml_parse function will report appropriate errors. If parsing succeeds, the function returns the original input data, relying on the fact that PostgreSQL xmltype and text are binary compatible internally.

## Parameters / Member Variables
- `data`: The text data to be parsed and validated as XML
- `xmloption_arg`: XML option type specifying parsing mode (XMLOPTION_DOCUMENT or XMLOPTION_CONTENT)
- `preserve_whitespace`: Boolean flag controlling whitespace preservation during parsing

## Dependencies
- Functions called/Symbols referenced:
  - xml_parse (core XML parsing and validation)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (database encoding retrieval)
  - [xmltype](xmltype.md) (data type for XML values)
- Called from (representative examples):
  - [ExecEvalXmlExpr](../E/ExecEvalXmlExpr.md) (expression evaluation in executor)
  - [texttoxml](../t/texttoxml.md) (text to XML conversion function)
  - PG_RETURN_XML_P (via macro usage)

## Notes and Other Information
- Requires libxml2 support (USE_LIBXML compilation flag)
- Performs validation only, does not transform the data
- Returns original input data after successful validation
- Relies on binary compatibility between text and xmltype
- Uses xml_parse for actual parsing and validation work
- Automatically frees parsed document after validation
- Falls back to NO_XML_SUPPORT() error when libxml2 is not available
- The preserve_whitespace parameter is passed through to xml_parse
- Located in src/backend/utils/adt/xml.c at lines 993-1010