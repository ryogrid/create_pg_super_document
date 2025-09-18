# xmlpi

## Location
src/backend/utils/adt/xml.c: 1011 - 1062

## Overview
Creates XML processing instructions with specified target names and optional content, implementing the SQL XMLPI function for PostgreSQL.

## Definition
xmltype *xmlpi(const char *target, text *arg, bool arg_is_null, bool *result_is_null)

## Detailed Description
The xmlpi function constructs XML processing instructions (PIs) following the XML specification and SQL standard. Processing instructions are special XML constructs that provide instructions to applications processing the XML document, formatted as "<?target content?>".

The function enforces XML well-formedness rules by rejecting "xml" as a target name (reserved by XML specification) and preventing the inclusion of the PI end sequence "?>" within the content. It follows SQL standard semantics for NULL handling, performing syntax validation before checking for NULL arguments.

When content is provided, the function strips leading whitespace and formats the PI with a space between target and content. The output is constructed using PostgreSQL StringInfo operations and converted to xmltype format for return.

## Parameters / Member Variables
- `target`: The target name for the processing instruction (cannot be "xml")
- `arg`: Optional text content for the processing instruction
- `arg_is_null`: Boolean flag indicating if the content argument is NULL
- `result_is_null`: Output parameter set to indicate if the result should be NULL

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (case-insensitive string comparison)
  - text_to_cstring (text to C string conversion)
  - [stringinfo_to_xmltype](../s/stringinfo_to_xmltype.md) (StringInfo to XML type conversion)
  - appendStringInfo/appendStringInfoChar/appendStringInfoString (string building)
  - initStringInfo (StringInfo initialization)
- Called from (representative examples):
  - [ExecEvalXmlExpr](../E/ExecEvalXmlExpr.md) (expression evaluation in executor)
  - PG_RETURN_XML_P (via macro usage)

## Notes and Other Information
- Requires libxml2 support (USE_LIBXML compilation flag)
- Enforces XML specification constraints on target names and content
- Follows SQL standard NULL handling semantics
- Automatically strips leading whitespace from content
- Validates that content does not contain the PI end sequence "?>"
- Returns NULL when arg_is_null is true (following SQL standard)
- Uses StringInfo for efficient string construction
- Proper memory management with pfree() calls for allocated strings
- Located in src/backend/utils/adt/xml.c at lines 1011-1062
- Target name "xml" is specifically forbidden per XML specification
- Processing instructions are commonly used for application-specific directives