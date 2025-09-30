# map_sql_value_to_xml_value

## Location
[src/backend/utils/adt/xml.c:2478-2696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2478-L2696)

## Overview
Converts SQL values to XML-compliant string representations according to SQL/XML:2008 section 9.8, with special formatting for various data types and optional character escaping.

## Definition

```c
struct_array(array, elmtype,
						  elmlen, elmbyval, elmalign,
						  &elem_values, &elem_nulls,
						  &num_elems);
```
## Detailed Description
This function converts PostgreSQL Datum values to their XML string representations, implementing the SQL/XML standard specifications. It handles various data types with specialized formatting requirements and provides optional character escaping for string values.

The function provides special handling for several data types:
- **Arrays**: Recursively processes each element, wrapping them in  tags
- **Boolean**: Converts to "true" or "false" strings (XSD format)
- **Date**: Formats using XSD date format, rejecting infinite values
- **Timestamp/TimestampTZ**: Formats using XSD datetime format with timezone support
- **Bytea**: Encodes as Base64 or BinHex (requires libxml2)
- **Other types**: Uses the type's native text output function

When  is true, special XML characters (&, <, >, etc.) are escaped to entity references. This parameter is typically false when used with libxml2 functions that perform their own escaping.

## Parameters
- : The Datum value to convert to XML format
- : The OID of the PostgreSQL data type
- : When true, escapes special XML characters in string values; when false, leaves them unescaped (useful with libxml2 functions that do their own escaping)

## Dependencies
- Functions called/Symbols referenced:
  - : Check if type is an array or array domain
  - : Extract array from Datum
  - : Get array element type
  - : Get type characteristics
  - : Break array into individual elements
  - : Get base type (flatten domains)
  - , , : Type-specific Datum extractors
  - : Convert Julian date to calendar date
  - , : Format dates/timestamps
  - : Convert timestamp to time structure
  - : Extract bytea from Datum
  - , : XML error context management
  - , : libxml2 buffer operations
  - , : Binary encoding functions
  - , : Generic type output
  - : XML character escaping function

- Called from (representative examples):
  - : XML expression evaluation in executor
  - : XML element construction
  - : Converting SQL rows to XML elements
  - : XPath result processing
  - Self-recursive calls for array element processing

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Supports both libxml2 and non-libxml2 builds (bytea encoding only available with libxml2)
- Rejects infinite date/timestamp values as they are not supported by XSD
- Uses XSD-compliant formatting for date/time types ()
- The  global variable controls whether bytea is encoded as Base64 or BinHex
- Properly handles PostgreSQL domains by flattening them to their base types
- XML type values are passed through unchanged to avoid double-processing
- Array processing uses recursive calls with forced string escaping enabled

## Simplified Source

```c
char *
map_sql_value_to_xml_value(Datum value, Oid type, bool xml_escape_strings)
{
    // Handle arrays by processing each element
    if (type_is_array_domain(type)) {
        ArrayType *array = DatumGetArrayTypeP(value);
        Oid elmtype = ARR_ELEMTYPE(array);
        int16 elmlen;
        bool elmbyval;
        char elmalign;
        int num_elems;
        Datum *elem_values;
        bool *elem_nulls;
        StringInfoData buf;

        get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
        deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
                         &elem_values, &elem_nulls, &num_elems);

        initStringInfo(&buf);

        // Wrap each non-null element in <element> tags
        for (int i = 0; i < num_elems; i++) {
            if (!elem_nulls[i]) {
                appendStringInfoString(&buf, "<element>");
                appendStringInfoString(&buf,
                    map_sql_value_to_xml_value(elem_values[i], elmtype, true));
                appendStringInfoString(&buf, "</element>");
            }
        }

        pfree(elem_values);
        pfree(elem_nulls);
        return buf.data;
    }

    // Flatten domains to base types
    type = getBaseType(type);

    // Special XSD formatting for specific data types
    switch (type) {
        case BOOLOID:
            return DatumGetBool(value) ? "true" : "false";

        case DATEOID: {
            DateADT date = DatumGetDateADT(value);
            struct pg_tm tm;
            char buf[MAXDATELEN + 1];

            // Reject infinite dates
            if (DATE_NOT_FINITE(date))
                ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                               errmsg("XML does not support infinite date values")));

            j2date(date + POSTGRES_EPOCH_JDATE, &tm.tm_year, &tm.tm_mon, &tm.tm_mday);
            EncodeDateOnly(&tm, USE_XSD_DATES, buf);
            return pstrdup(buf);
        }

        case TIMESTAMPOID: {
            Timestamp timestamp = DatumGetTimestamp(value);
            struct pg_tm tm;
            fsec_t fsec;
            char buf[MAXDATELEN + 1];

            // Reject infinite timestamps
            if (TIMESTAMP_NOT_FINITE(timestamp))
                ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                               errmsg("XML does not support infinite timestamp values")));

            if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL) == 0)
                EncodeDateTime(&tm, fsec, false, 0, NULL, USE_XSD_DATES, buf);
            else
                ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                               errmsg("timestamp out of range")));

            return pstrdup(buf);
        }

        case TIMESTAMPTZOID: {
            TimestampTz timestamp = DatumGetTimestamp(value);
            struct pg_tm tm;
            int tz;
            fsec_t fsec;
            const char *tzn = NULL;
            char buf[MAXDATELEN + 1];

            // Similar processing with timezone info
            if (TIMESTAMP_NOT_FINITE(timestamp))
                ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                               errmsg("XML does not support infinite timestamp values")));

            if (timestamp2tm(timestamp, &tz, &tm, &fsec, &tzn, NULL) == 0)
                EncodeDateTime(&tm, fsec, true, tz, tzn, USE_XSD_DATES, buf);
            else
                ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                               errmsg("timestamp out of range")));

            return pstrdup(buf);
        }

#ifdef USE_LIBXML
        case BYTEAOID: {
            // Encode bytea as Base64 or BinHex using libxml2
            bytea *bstr = DatumGetByteaPP(value);
            // ... libxml2 encoding logic (simplified) ...
            // Returns encoded string
        }
#endif
    }

    // For all other types, use native text representation
    Oid typeOut;
    bool isvarlena;
    char *str;

    getTypeOutputInfo(type, &typeOut, &isvarlena);
    str = OidOutputFunctionCall(typeOut, value);

    // Return as-is for XML type or when escaping disabled
    if (type == XMLOID || !xml_escape_strings)
        return str;

    // Otherwise escape XML special characters
    return escape_xml(str);
}
```