# print_xml_decl

## Location
[src/backend/utils/adt/xml.c:1606-1671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1606-L1671)

## Overview
Generates and appends an XML declaration to a StringInfo buffer following SQL standard serialization rules, avoiding unnecessary declarations when possible.

## Definition

```c
structions.
 * This function need only return true if it sees a valid sequence of such
 * things leading to <!DOCTYPE.  It can simply return false in any other
 * cases, including malformed input;
```
## Detailed Description
This function implements the "Serialization of an XML value" clause from the SQL standard by generating XML declarations only when necessary. The function follows a minimalist approach to avoid cluttering simple XML output with redundant declarations.

The decision logic for generating a declaration:
- Always generate if standalone property is specified (not -1)
- Always generate if encoding is specified and not UTF-8
- Always generate if version is specified and not "1.0" (the default)
- Otherwise, omit the declaration to keep output clean

When a declaration is generated, it follows XML 1.0 specification format with proper quoting and attribute ordering. The version attribute is always included (as required by XML), defaulting to "1.0" if not specified.

## Parameters / Member Variables
- : StringInfo buffer to append the XML declaration to
- : XML version string (typically "1.0"), or NULL to use default
- : PostgreSQL encoding identifier for the document encoding
- : Standalone flag (1=yes, 0=no, -1=not specified/omit)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (string comparison for version check)
  - PG_XML_DEFAULT_VERSION (default XML version constant "1.0")
  - PG_UTF8 (UTF-8 encoding identifier constant)
  - [appendStringInfoString](../a/appendStringInfoString.md) (append string to StringInfo buffer)
  - [appendStringInfo](../a/appendStringInfo.md) (formatted append to StringInfo buffer)  
  - [pg_encoding_to_char](pg_encoding_to_char.md) (convert PostgreSQL encoding to string name)
- Called from (representative examples):
  - [xml_out_internal](../x/xml_out_internal.md) (XML output processing)
  - [xmlconcat](../x/xmlconcat.md) (XML concatenation operations)
  - [xmlroot](../x/xmlroot.md) (XML root element processing)

## Notes and Other Information
- Returns true if declaration was written, false if omitted
- Function is static (internal to xml.c file)
- Implements SQL:2003 standard behavior for XML serialization
- Uses double quotes for all attribute values in the declaration
- Encoding names use PostgreSQL internal names (may want IANA names in future)
- Minimizes XML declaration generation to avoid verbose output for simple cases
- Always includes version attribute when declaration is present (XML requirement)
- UTF-8 encoding is considered default and doesn't trigger declaration generation

## Simplified Source

```c
static bool
print_xml_decl(StringInfo buf, const xmlChar *version,
               pg_enc encoding, int standalone)
{
    // Only generate declaration if non-default values are present
    if ((version && strcmp((const char *) version, PG_XML_DEFAULT_VERSION) != 0) ||
        (encoding && encoding != PG_UTF8) ||
        standalone != -1) {

        // Start XML declaration
        appendStringInfoString(buf, "<?xml");

        // Add version attribute (required when declaration is present)
        if (version)
            appendStringInfo(buf, " version=\"%s\"", version);
        else
            appendStringInfo(buf, " version=\"%s\"", PG_XML_DEFAULT_VERSION);

        // Add encoding if specified and not UTF-8
        if (encoding && encoding != PG_UTF8) {
            appendStringInfo(buf, " encoding=\"%s\"",
                           pg_encoding_to_char(encoding));
        }

        // Add standalone attribute if specified
        if (standalone == 1)
            appendStringInfoString(buf, " standalone=\"yes\"");
        else if (standalone == 0)
            appendStringInfoString(buf, " standalone=\"no\"");

        // Close declaration
        appendStringInfoString(buf, "?>");
        return true;
    }
    else {
        // No declaration needed for default values
        return false;
    }
}
```