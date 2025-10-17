# map_sql_type_to_xmlschema_type

## Location
[src/backend/utils/adt/xml.c:3911-4085](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L3911-L4085)

## Overview
Maps PostgreSQL data types to complete XML Schema type definitions with restrictions and constraints according to SQL/XML:2008 sections 9.5 and 9.6.

## Definition
```c
static const char *
map_sql_type_to_xmlschema_type(Oid typeoid, int typmod)
```

## Detailed Description
This function generates complete XML Schema type definitions for PostgreSQL data types, creating detailed `<xsd:simpleType>` or `<xsd:complexType>` elements with appropriate restrictions, constraints, and validation patterns. Unlike `map_sql_type_to_xml_name` which returns simple type names, this function generates full XML Schema type definitions including base types, restrictions, and validation rules.

The function handles special cases:
- XML types: Creates complex types with mixed content and flexible element sequences
- Built-in types: Creates simple types with appropriate base restrictions and constraints
- Domain types: Creates restrictions based on the underlying base type
- Numeric types: Includes precision, scale, and range constraints
- String types: Includes length restrictions
- Date/time types: Includes detailed pattern validation using regular expressions

## Parameters / Member Variables
- `typeoid`: The PostgreSQL OID of the data type to map
- `typmod`: Type modifier containing additional type information (size, precision, scale, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [map_sql_type_to_xml_name](map_sql_type_to_xml_name.md) (for generating type names and domain base types)
  - [get_typtype](../g/get_typtype.md) (PostgreSQL function to determine type category)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md) (PostgreSQL function for resolving domain base types)
  - TYPTYPE_DOMAIN (PostgreSQL constant for domain types)
  - XMLBINARY_BASE64 (XML binary encoding option)
  - Various PostgreSQL constants: INT64_FORMAT, PG_INT64_MAX, PG_INT64_MIN, etc.
- Called from (representative examples):
  - [map_sql_typecoll_to_xmlschema_types](map_sql_typecoll_to_xmlschema_types.md)

## Notes and Other Information
- Creates comprehensive XML Schema definitions with detailed validation:
  - [String](../S/String.md) types: `xsd:maxLength` restrictions for VARCHAR/CHAR with length limits
  - [Numeric](../N/Numeric.md) types: `xsd:totalDigits` and `xsd:fractionDigits` for NUMERIC types
  - [Integer](../I/Integer.md) types: `xsd:maxInclusive` and `xsd:minInclusive` range constraints
  - Binary types: Base64Binary or hexBinary encoding based on configuration
  - Date/time types: Complex regex patterns for format validation including timezone handling
- XML types receive special treatment with `mixed="true"` complex types allowing arbitrary content
- Domain types are mapped by creating restrictions on their base types
- Uses precise regular expression patterns for temporal types with optional fractional seconds
- The function is static and only used internally within the xml.c module
- Memory management relies on StringInfo for building complex XML Schema strings
- Implements both SQL/XML:2008 section 9.5 (unnamed types) and 9.6 (named types) with name attributes

## Simplified Source

```c
static const char *map_sql_type_to_xmlschema_type(Oid typeoid, int typmod) {
    StringInfoData result;
    const char *typename = map_sql_type_to_xml_name(typeoid, typmod);
    initStringInfo(&result);

    // XML type gets special complex type treatment
    if (typeoid == XMLOID) {
        appendStringInfoString(&result,
            "<xsd:complexType mixed=\"true\">\n"
            "  <xsd:sequence>\n"
            "    <xsd:any name=\"element\" minOccurs=\"0\" maxOccurs=\"unbounded\" processContents=\"skip\"/>\n"
            "  </xsd:sequence>\n"
            "</xsd:complexType>\n");
    } else {
        // All other types use simple type with restrictions
        appendStringInfo(&result, "<xsd:simpleType name=\"%s\">\n", typename);

        switch (typeoid) {
            case BPCHAROID:
            case VARCHAROID:
            case TEXTOID:
                appendStringInfoString(&result, "  <xsd:restriction base=\"xsd:string\">\n");
                if (typmod != -1)
                    appendStringInfo(&result, "    <xsd:maxLength value=\"%d\"/>\n", typmod - VARHDRSZ);
                appendStringInfoString(&result, "  </xsd:restriction>\n");
                break;

            case BYTEAOID:
                appendStringInfo(&result, "  <xsd:restriction base=\"xsd:%s\">\n  </xsd:restriction>\n",
                    xmlbinary == XMLBINARY_BASE64 ? "base64Binary" : "hexBinary");
                break;

            case NUMERICOID:
                if (typmod != -1)
                    appendStringInfo(&result,
                        "  <xsd:restriction base=\"xsd:decimal\">\n"
                        "    <xsd:totalDigits value=\"%d\"/>\n"
                        "    <xsd:fractionDigits value=\"%d\"/>\n"
                        "  </xsd:restriction>\n",
                        ((typmod - VARHDRSZ) >> 16) & 0xffff,
                        (typmod - VARHDRSZ) & 0xffff);
                break;

            case INT2OID:
                appendStringInfo(&result,
                    "  <xsd:restriction base=\"xsd:short\">\n"
                    "    <xsd:maxInclusive value=\"%d\"/>\n"
                    "    <xsd:minInclusive value=\"%d\"/>\n"
                    "  </xsd:restriction>\n", SHRT_MAX, SHRT_MIN);
                break;

            case INT4OID:
                appendStringInfo(&result,
                    "  <xsd:restriction base=\"xsd:int\">\n"
                    "    <xsd:maxInclusive value=\"%d\"/>\n"
                    "    <xsd:minInclusive value=\"%d\"/>\n"
                    "  </xsd:restriction>\n", INT_MAX, INT_MIN);
                break;

            case INT8OID:
                appendStringInfo(&result,
                    "  <xsd:restriction base=\"xsd:long\">\n"
                    "    <xsd:maxInclusive value=\"" INT64_FORMAT "\"/>\n"
                    "    <xsd:minInclusive value=\"" INT64_FORMAT "\"/>\n"
                    "  </xsd:restriction>\n", PG_INT64_MAX, PG_INT64_MIN);
                break;

            case FLOAT4OID:
                appendStringInfoString(&result, "  <xsd:restriction base=\"xsd:float\"></xsd:restriction>\n");
                break;

            case FLOAT8OID:
                appendStringInfoString(&result, "  <xsd:restriction base=\"xsd:double\"></xsd:restriction>\n");
                break;

            case BOOLOID:
                appendStringInfoString(&result, "  <xsd:restriction base=\"xsd:boolean\"></xsd:restriction>\n");
                break;

            case TIMEOID:
            case TIMETZOID:
                // Create time pattern with optional timezone
                const char *tz = (typeoid == TIMETZOID ? "(\\\\+|-)\\\\p{Nd}{2}:\\\\p{Nd}{2}" : "");
                appendStringInfo(&result,
                    "  <xsd:restriction base=\"xsd:time\">\n"
                    "    <xsd:pattern value=\"\\\\p{Nd}{2}:\\\\p{Nd}{2}:\\\\p{Nd}{2}(.\\\\p{Nd}+)?%s\"/>\n"
                    "  </xsd:restriction>\n", tz);
                break;

            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
                // Create datetime pattern with optional timezone
                tz = (typeoid == TIMESTAMPTZOID ? "(\\\\+|-)\\\\p{Nd}{2}:\\\\p{Nd}{2}" : "");
                appendStringInfo(&result,
                    "  <xsd:restriction base=\"xsd:dateTime\">\n"
                    "    <xsd:pattern value=\"\\\\p{Nd}{4}-\\\\p{Nd}{2}-\\\\p{Nd}{2}T\\\\p{Nd}{2}:\\\\p{Nd}{2}:\\\\p{Nd}{2}(.\\\\p{Nd}+)?%s\"/>\n"
                    "  </xsd:restriction>\n", tz);
                break;

            case DATEOID:
                appendStringInfoString(&result,
                    "  <xsd:restriction base=\"xsd:date\">\n"
                    "    <xsd:pattern value=\"\\\\p{Nd}{4}-\\\\p{Nd}{2}-\\\\p{Nd}{2}\"/>\n"
                    "  </xsd:restriction>\n");
                break;

            default:
                // Handle domain types by restricting base type
                if (get_typtype(typeoid) == TYPTYPE_DOMAIN) {
                    Oid base_typeoid;
                    int32 base_typmod = -1;
                    base_typeoid = getBaseTypeAndTypmod(typeoid, &base_typmod);
                    appendStringInfo(&result, "  <xsd:restriction base=\"%s\"/>\n",
                        map_sql_type_to_xml_name(base_typeoid, base_typmod));
                }
                break;
        }
        appendStringInfoString(&result, "</xsd:simpleType>\n");
    }

    return result.data;
}
```