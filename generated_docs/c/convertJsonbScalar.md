# convertJsonbScalar

## Location
[src/backend/utils/adt/jsonb_util.c:1821-1885](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1821-L1885)

## Overview
Converts a JsonbValue scalar (leaf) value into its binary JSONB representation, handling all primitive JSON data types including null, string, numeric, boolean, and datetime values.

## Definition

```c
static void
convertJsonbScalar(StringInfo buffer, JEntry *header, JsonbValue *scalarVal)
```
## Detailed Description
The  function is responsible for converting leaf-level JSON values into their binary JSONB representation. It handles all primitive JSON data types by:

1. Examining the scalar type via 
2. For each type, serializing the data appropriately into the buffer
3. Setting the correct JEntry header flags and length information
4. Handling special cases like numeric alignment and datetime encoding

The function serves as the foundation for JSONB's binary format by properly encoding each scalar value type with its corresponding metadata, ensuring efficient storage and retrieval.

## Parameters / Member Variables
- `buffer`: StringInfo buffer where the serialized scalar data will be appended
- `*header`: Pointer to JEntry that will be filled with type flags and length information for this scalar
- `*scalarVal`: JsonbValue containing the scalar data to be converted
## Dependencies
- Functions called/Symbols referenced:
  - [appendToBuffer](../a/appendToBuffer.md) (appends string and numeric data to buffer)
  - [padBufferToInt](../p/padBufferToInt.md) (aligns buffer for numeric values)
  - [JsonEncodeDateTime](../J/JsonEncodeDateTime.md) (converts datetime values to string representation)
  - VARSIZE_ANY (gets size of PostgreSQL varlena types)
- Constants used:
  - JENTRY_ISNULL (null value flag)
  - JENTRY_ISNUMERIC (numeric value flag)
  - JENTRY_ISBOOL_TRUE/JENTRY_ISBOOL_FALSE (boolean value flags)
  - MAXDATELEN (maximum datetime string length)
- JSON value types handled:
  - jbvNull (JSON null)
  - jbvString (JSON string)
  - jbvNumeric (JSON number)
  - jbvBool (JSON boolean)
  - [jbvDatetime](../j/jbvDatetime.md) (PostgreSQL datetime extension)
- Called from:
  - [convertJsonbValue](convertJsonbValue.md) (main conversion dispatcher for scalars)
  - [convertJsonbObject](convertJsonbObject.md) (for object keys, which must be strings)

## Notes and Other Information
- Handles all JSON primitive data types plus PostgreSQL's datetime extension
- [String](../S/String.md) values are stored directly with their length in the JEntry header
- [Numeric](../N/Numeric.md) values require 4-byte alignment via  and include padding in their length calculation
- [Boolean](../B/Boolean.md) values don't store data in the buffer, only flags in the JEntry header
- Datetime values are converted to string representation using 
- Null values only set a flag in the JEntry header without storing data
- The function includes error handling for invalid scalar types
- Critical for JSONB's efficient binary storage format where type information is encoded in JEntry headers
- Static function used internally within the JSONB conversion system

## Simplified Source

```c
static void
convertJsonbScalar(StringInfo buffer, JEntry *header, JsonbValue *scalarVal)
{
    switch (scalarVal->type) {
        case jbvNull:
            // Null values: only set flag, no data stored
            *header = JENTRY_ISNULL;
            break;

        case jbvString:
            // String values: append data and store length
            appendToBuffer(buffer, scalarVal->val.string.val, scalarVal->val.string.len);
            *header = scalarVal->val.string.len;
            break;

        case jbvNumeric:
            // Numeric values: align buffer and include padding in length
            int numlen = VARSIZE_ANY(scalarVal->val.numeric);
            short padlen = padBufferToInt(buffer);

            appendToBuffer(buffer, (char *) scalarVal->val.numeric, numlen);
            *header = JENTRY_ISNUMERIC | (padlen + numlen);
            break;

        case jbvBool:
            // Boolean values: only set flag, no data stored
            *header = (scalarVal->val.boolean) ?
                JENTRY_ISBOOL_TRUE : JENTRY_ISBOOL_FALSE;
            break;

        case jbvDatetime:
            // Datetime values: convert to string representation
            char buf[MAXDATELEN + 1];
            JsonEncodeDateTime(buf,
                             scalarVal->val.datetime.value,
                             scalarVal->val.datetime.typid,
                             &scalarVal->val.datetime.tz);

            size_t len = strlen(buf);
            appendToBuffer(buffer, buf, len);
            *header = len;
            break;

        default:
            elog(ERROR, "invalid jsonb scalar type");
    }
}
```