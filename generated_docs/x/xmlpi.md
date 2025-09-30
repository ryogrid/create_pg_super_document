# xmlpi

## Location
[src/backend/utils/adt/xml.c:1011-1062](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1011-L1062)

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
  - [text_to_cstring](../t/text_to_cstring.md) (text to C string conversion)
  - [stringinfo_to_xmltype](../s/stringinfo_to_xmltype.md) (StringInfo to XML type conversion)
  - [appendStringInfo](../a/appendStringInfo.md)/appendStringInfoChar/appendStringInfoString (string building)
  - [initStringInfo](../i/initStringInfo.md) (StringInfo initialization)
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

## Simplified Source

```c
xmltype *xmlpi(const char *target, text *arg, bool arg_is_null, bool *result_is_null)
{
#ifdef USE_LIBXML
    xmltype *result;
    StringInfoData buf;

    // Validate target name - "xml" is reserved
    if (pg_strcasecmp(target, "xml") == 0)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("invalid XML processing instruction"),
                errdetail("XML processing instruction target name cannot be \"%s\".", target)));

    // Handle NULL content (SQL standard: syntax check before NULL check)
    *result_is_null = arg_is_null;
    if (*result_is_null)
        return NULL;

    initStringInfo(&buf);

    // Build processing instruction: <?target content?>
    appendStringInfo(&buf, "<?%s", target);

    if (arg != NULL)
    {
        char *string = text_to_cstring(arg);

        // Validate content doesn't contain PI end sequence
        if (strstr(string, "?>") != NULL)
            ereport(ERROR, (errcode(ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION),
                    errmsg("invalid XML processing instruction"),
                    errdetail("XML processing instruction cannot contain \"?>\".")));

        // Add space and content (strip leading whitespace)
        appendStringInfoChar(&buf, ' ');
        appendStringInfoString(&buf, string + strspn(string, " "));
        pfree(string);
    }

    appendStringInfoString(&buf, "?>");

    result = stringinfo_to_xmltype(&buf);
    pfree(buf.data);
    return result;
#else
    NO_XML_SUPPORT();
    return NULL;
#endif
}
```