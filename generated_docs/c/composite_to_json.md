# composite_to_json

## Location
[src/backend/utils/adt/json.c:512-592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L512-L592)

## Overview
Converts PostgreSQL composite types (records/rows) into JSON object format by iterating through tuple attributes and converting each field to its appropriate JSON representation.

## Definition
```c
static void composite_to_json(Datum composite, StringInfo result, bool use_line_feeds)
```

## Detailed Description
composite_to_json transforms PostgreSQL composite types (such as table rows or custom record types) into JSON objects. It extracts the tuple descriptor from the composite type, builds a temporary HeapTuple structure for attribute access, and iterates through each non-dropped attribute. For each attribute, it escapes the attribute name as a JSON key, retrieves the attribute value using heap_getattr, determines the appropriate JSON type category, and converts the value using datum_to_json_internal. The function constructs proper JSON object syntax with curly braces, quoted keys, colons, and comma separators, with optional line feeds for formatting.

## Parameters / Member Variables
- `composite`: PostgreSQL Datum containing the composite type value to convert
- `result`: StringInfo buffer where the JSON output is accumulated
- `use_line_feeds`: Boolean controlling whether to add line feeds for pretty formatting

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetHeapTupleHeader (extract tuple header from Datum)
  - HeapTupleHeaderGetTypeId, HeapTupleHeaderGetTypMod (get type information)
  - HeapTupleHeaderGetDatumLength (get tuple length)
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md) (get tuple descriptor for the row type)
  - [heap_getattr](../h/heap_getattr.md) (extract attribute value from tuple)
  - [escape_json](../e/escape_json.md) (properly escape attribute names for JSON)
  - [json_categorize_type](../j/json_categorize_type.md) (determine JSON conversion approach)
  - [datum_to_json_internal](../d/datum_to_json_internal.md) (convert individual attribute values)
  - ReleaseTupleDesc (memory cleanup)
- Called from (representative examples):
  - [datum_to_json_internal](../d/datum_to_json_internal.md)
  - [row_to_json](../r/row_to_json.md)
  - [row_to_json_pretty](../r/row_to_json_pretty.md)

## Notes and Other Information
The function skips dropped attributes (attisdropped) to avoid including deleted columns in the JSON output. It optimizes separator handling by pre-calculating separator lengths to avoid repeated strlen() calls. NULL values are handled specially with JSONTYPE_NULL category and InvalidOid output function. The function properly manages the tuple descriptor lifecycle by calling ReleaseTupleDesc for cleanup. The needsep flag ensures proper comma placement between object members.

## Simplified Source

```c
static void composite_to_json(Datum composite, StringInfo result, bool use_line_feeds) {
    HeapTupleHeader td;
    Oid tupType;
    int32 tupTypmod;
    TupleDesc tupdesc;
    HeapTupleData tmptup, *tuple;
    int i;
    bool needsep = false;
    const char *sep;
    int seplen;

    // Set up separator for formatting
    sep = use_line_feeds ? ",\n " : ",";
    seplen = use_line_feeds ? strlen(",\n ") : strlen(",");

    // Extract tuple metadata
    td = DatumGetHeapTupleHeader(composite);
    tupType = HeapTupleHeaderGetTypeId(td);
    tupTypmod = HeapTupleHeaderGetTypMod(td);
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

    // Build temporary HeapTuple structure
    tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
    tmptup.t_data = td;
    tuple = &tmptup;

    appendStringInfoChar(result, '{');

    // Process each attribute
    for (i = 0; i < tupdesc->natts; i++) {
        Datum val;
        bool isnull;
        char *attname;
        JsonTypeCategory tcategory;
        Oid outfuncoid;
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);

        // Skip dropped attributes
        if (att->attisdropped)
            continue;

        // Add separator if needed
        if (needsep)
            appendBinaryStringInfo(result, sep, seplen);
        needsep = true;

        // Add attribute name as JSON key
        attname = NameStr(att->attname);
        escape_json(result, attname);
        appendStringInfoChar(result, ':');

        // Get attribute value
        val = heap_getattr(tuple, i + 1, tupdesc, &isnull);

        // Determine JSON conversion approach
        if (isnull) {
            tcategory = JSONTYPE_NULL;
            outfuncoid = InvalidOid;
        } else {
            json_categorize_type(att->atttypid, false, &tcategory, &outfuncoid);
        }

        // Convert value to JSON
        datum_to_json_internal(val, isnull, result, tcategory, outfuncoid, false);
    }

    appendStringInfoChar(result, '}');
    ReleaseTupleDesc(tupdesc);
}
```