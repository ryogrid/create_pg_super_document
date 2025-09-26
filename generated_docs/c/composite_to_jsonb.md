# composite_to_jsonb

## Location
[src/backend/utils/adt/jsonb.c:942-1015](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L942-L1015)

## Overview
Converts a PostgreSQL composite type (record/row) datum into JSONB object format, mapping each field to a key-value pair.

## Definition
```c
static void composite_to_jsonb(Datum composite, JsonbInState *result)
```

## Detailed Description
The `composite_to_jsonb` function converts PostgreSQL composite types (also known as records or rows) into JSONB objects. It processes the composite datum through several stages:

1. **Header Extraction**: Extracts the HeapTupleHeader from the composite datum and retrieves type information
2. **Type Resolution**: Uses the tuple type OID and typmod to look up the corresponding TupleDesc structure
3. **Tuple Setup**: Creates a temporary HeapTuple control structure for accessing individual fields
4. **Object Creation**: Begins a JSONB object container
5. **Field Processing**: Iterates through each attribute in the tuple descriptor:
   - Skips dropped attributes (attisdropped = true)
   - Uses the attribute name as the JSONB object key
   - Extracts the field value using heap_getattr
   - Handles null values by setting appropriate type category
   - For non-null values, determines the JSON type category
   - Recursively converts the field value using datum_to_jsonb_internal
6. **Object Completion**: Closes the JSONB object and releases the tuple descriptor

The function preserves the structure and field names of the original composite type while converting field values to appropriate JSONB representations.

## Parameters / Member Variables
- `composite`: Datum containing the PostgreSQL composite type to be converted
- `result`: JsonbInState structure to accumulate the conversion result

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetHeapTupleHeader
  - HeapTupleHeaderGetTypeId
  - HeapTupleHeaderGetTypMod
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md)
  - HeapTupleHeaderGetDatumLength
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - TupleDescAttr
  - NameStr
  - [heap_getattr](../h/heap_getattr.md)
  - [json_categorize_type](../j/json_categorize_type.md)
  - [datum_to_jsonb_internal](../d/datum_to_jsonb_internal.md)
  - ReleaseTupleDesc
- Called from (representative examples):
  - [datum_to_jsonb_internal](../d/datum_to_jsonb_internal.md)

## Notes and Other Information
- This is a static function used internally within jsonb.c for composite type conversion
- Handles dropped attributes by skipping them entirely in the output JSONB object
- Attribute names are used directly as JSONB object keys without length validation since they cannot exceed PostgreSQL's maximum name length
- Uses heap_getattr to safely extract field values, handling both fixed-length and variable-length attributes
- Null field values are handled specially with JSONTYPE_NULL category and InvalidOid for the output function
- The function properly manages the tuple descriptor by calling ReleaseTupleDesc to avoid memory leaks
- Field values are recursively processed through datum_to_jsonb_internal, allowing nested composite types and arrays to be properly converted
- The resulting JSONB object preserves the original field order from the composite type definition