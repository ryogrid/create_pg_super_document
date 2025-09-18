# wellformed_xml

## Location
src/backend/utils/adt/xml.c: 4591 - 4608

## Overview
Internal static function that checks whether given text data represents well-formed XML according to specified XML processing options.

## Definition
```c
static bool wellformed_xml(text *data, XmlOptionType xmloption_arg)
```

## Detailed Description
The `wellformed_xml` function is a core utility function that validates XML well-formedness by attempting to parse the provided text data using libxml2. It uses PostgreSQL's error handling context mechanism to capture parsing errors without raising exceptions, allowing it to return a simple boolean result indicating whether the XML is well-formed.

The function creates an ErrorSaveContext to capture any parsing errors, then calls `xml_parse` with the provided data and XML options. If parsing succeeds without errors, the XML is considered well-formed. The function properly cleans up any allocated XML document structure before returning.

## Parameters / Member Variables
- `data`: Text data containing the XML content to validate
- `xmloption_arg`: XmlOptionType enum value specifying XML processing behavior (document vs content mode)

## Dependencies
- Functions called/Symbols referenced:
  - xml_parse
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - xmlFreeDoc (libxml2 function)
  - [ErrorSaveContext](../E/ErrorSaveContext.md)
  - [XmlOptionType](../X/XmlOptionType.md)
- Called from (representative examples):
  - [xml_is_well_formed](../x/xml_is_well_formed.md)
  - [xml_is_well_formed_document](../x/xml_is_well_formed_document.md)
  - [xml_is_well_formed_content](../x/xml_is_well_formed_content.md)

## Notes and Other Information
- Internal static function not directly accessible from SQL
- Uses PostgreSQL's ErrorSaveContext mechanism for soft error handling
- Properly manages memory by freeing parsed XML documents
- Located in src/backend/utils/adt/xml.c:4591-4608
- Returns true if XML parsing succeeds without errors, false otherwise
- Part of PostgreSQL's XML validation infrastructure
- Serves as the common implementation for public well-formedness checking functions