# datum_to_jsonb_internal

## Location
[src/backend/utils/adt/jsonb.c:638-861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L638-L861)

## Overview
Converts a PostgreSQL Datum value into a JsonbValue and adds it to the result JsonbInState, handling all data types that can be represented in JSONB format.

## Definition
```c
static void datum_to_jsonb_internal(Datum val, bool is_null, JsonbInState *result,
                                   JsonTypeCategory tcategory, Oid outfuncoid,
                                   bool key_scalar)
```

## Detailed Description
The `datum_to_jsonb_internal` function is a core component of PostgreSQL's JSONB conversion system. It takes a Datum value along with its type information and converts it into the appropriate JsonbValue representation, then integrates it into the JsonbInState parsing structure. The function handles a wide variety of PostgreSQL data types including scalars, arrays, composite types, and existing JSON/JSONB values.

The function performs different conversions based on the JsonTypeCategory:
- **JSONTYPE_ARRAY**: Delegates to array_to_jsonb_internal for array processing
- **JSONTYPE_COMPOSITE**: Delegates to composite_to_jsonb for record/composite types
- **JSONTYPE_BOOL**: Converts boolean values, with special handling for key contexts
- **JSONTYPE_NUMERIC**: Handles numeric values with validation for JSON number format
- **JSONTYPE_DATE/TIMESTAMP/TIMESTAMPTZ**: Converts temporal types to ISO string format
- **JSONTYPE_JSON/JSONB**: Parses existing JSON text or processes JSONB structures
- **Default**: Converts other types to strings using their output functions

Special considerations include key_scalar mode where certain complex types are rejected for use as object keys, and proper handling of null values and scalar JSONB roots.

## Parameters / Member Variables
- `val`: The Datum value to be converted to JSONB
- `is_null`: Boolean indicating if the value is SQL NULL
- `result`: JsonbInState structure to accumulate the conversion result
- `tcategory`: JsonTypeCategory indicating the data type classification
- `outfuncoid`: OID of the output function for the data type
- `key_scalar`: Boolean flag indicating if this value will be used as an object key

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - [array_to_jsonb_internal](../a/array_to_jsonb_internal.md)
  - [composite_to_jsonb](../c/composite_to_jsonb.md)
  - OidFunctionCall1
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md)
  - [DatumGetBool](../D/DatumGetBool.md)
  - DirectFunctionCall3
  - [numeric_in](../n/numeric_in.md)
  - [DatumGetNumeric](../D/DatumGetNumeric.md)
  - [JsonEncodeDateTime](../J/JsonEncodeDateTime.md)
  - DatumGetTextPP
  - [makeJsonLexContext](../m/makeJsonLexContext.md)
  - pg_parse_json_or_ereport
  - [freeJsonLexContext](../f/freeJsonLexContext.md)
  - [DatumGetJsonbP](../D/DatumGetJsonbP.md)
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [checkStringLen](../c/checkStringLen.md)
- Called from (representative examples):
  - [array_dim_to_jsonb](../a/array_dim_to_jsonb.md)
  - [composite_to_jsonb](../c/composite_to_jsonb.md)
  - [add_jsonb](../a/add_jsonb.md)
  - [datum_to_jsonb](datum_to_jsonb.md)
  - [jsonb_agg_transfn_worker](../j/jsonb_agg_transfn_worker.md)
  - [jsonb_object_agg_transfn_worker](../j/jsonb_object_agg_transfn_worker.md)

## Notes and Other Information
- This is a static function used internally within jsonb.c for type conversion
- Assumes result->escontext is NULL and errors will be thrown rather than handled gracefully
- Key scalar mode enforces restrictions on complex types (arrays, composites, JSON) to prevent invalid object keys
- For numeric types, performs validation to ensure the string representation is valid JSON (checking for 'N' or 'n' characters)
- Handles both scalar and non-scalar JSONB inputs with different processing paths
- Uses recursive parsing for JSON text input and iterative processing for existing JSONB structures
- The function is designed to be part of a larger parsing state machine managed by JsonbInState

## Simplified Source

```c
static void datum_to_jsonb_internal(Datum val, bool is_null, JsonbInState *result,
                                   JsonTypeCategory tcategory, Oid outfuncoid,
                                   bool key_scalar) {
    char *outputstr;
    bool numeric_error;
    JsonbValue jb;
    bool scalar_jsonb = false;

    check_stack_depth();

    // Handle null values
    if (is_null) {
        Assert(!key_scalar);
        jb.type = jbvNull;
    }
    // Reject complex types as keys
    else if (key_scalar && (tcategory == JSONTYPE_ARRAY ||
                           tcategory == JSONTYPE_COMPOSITE ||
                           tcategory == JSONTYPE_JSON ||
                           tcategory == JSONTYPE_JSONB ||
                           tcategory == JSONTYPE_CAST)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("key value must be scalar, not array, composite, or json")));
    }
    else {
        // Apply cast function if needed
        if (tcategory == JSONTYPE_CAST)
            val = OidFunctionCall1(outfuncoid, val);

        // Convert based on type category
        switch (tcategory) {
            case JSONTYPE_ARRAY:
                array_to_jsonb_internal(val, result);
                break;

            case JSONTYPE_COMPOSITE:
                composite_to_jsonb(val, result);
                break;

            case JSONTYPE_BOOL:
                if (key_scalar) {
                    // Keys must be strings, convert boolean to string
                    outputstr = DatumGetBool(val) ? "true" : "false";
                    jb.type = jbvString;
                    jb.val.string.len = strlen(outputstr);
                    jb.val.string.val = outputstr;
                } else {
                    jb.type = jbvBool;
                    jb.val.boolean = DatumGetBool(val);
                }
                break;

            case JSONTYPE_NUMERIC:
                outputstr = OidOutputFunctionCall(outfuncoid, val);
                if (key_scalar) {
                    // Always quote keys
                    jb.type = jbvString;
                    jb.val.string.len = strlen(outputstr);
                    jb.val.string.val = outputstr;
                } else {
                    // Check if valid JSON number (no 'N' or 'n' for NaN/Infinity)
                    numeric_error = (strchr(outputstr, 'N') != NULL ||
                                   strchr(outputstr, 'n') != NULL);
                    if (!numeric_error) {
                        // Valid number, store as numeric
                        Datum numd = DirectFunctionCall3(numeric_in,
                                                        CStringGetDatum(outputstr),
                                                        ObjectIdGetDatum(InvalidOid),
                                                        Int32GetDatum(-1));
                        jb.type = jbvNumeric;
                        jb.val.numeric = DatumGetNumeric(numd);
                        pfree(outputstr);
                    } else {
                        // Invalid number, store as string
                        jb.type = jbvString;
                        jb.val.string.len = strlen(outputstr);
                        jb.val.string.val = outputstr;
                    }
                }
                break;

            case JSONTYPE_DATE:
            case JSONTYPE_TIMESTAMP:
            case JSONTYPE_TIMESTAMPTZ:
                // Convert temporal types to ISO string format
                jb.type = jbvString;
                jb.val.string.val = JsonEncodeDateTime(NULL, val,
                    (tcategory == JSONTYPE_DATE) ? DATEOID :
                    (tcategory == JSONTYPE_TIMESTAMP) ? TIMESTAMPOID : TIMESTAMPTZOID,
                    NULL);
                jb.val.string.len = strlen(jb.val.string.val);
                break;

            case JSONTYPE_CAST:
            case JSONTYPE_JSON:
                // Parse JSON text directly into result structure
                {
                    JsonLexContext lex;
                    JsonSemAction sem;
                    text *json = DatumGetTextPP(val);

                    makeJsonLexContext(&lex, json, true);
                    memset(&sem, 0, sizeof(sem));
                    sem.semstate = (void *) result;
                    // Set up parsing callbacks for building JSONB
                    sem.object_start = jsonb_in_object_start;
                    sem.array_start = jsonb_in_array_start;
                    sem.object_end = jsonb_in_object_end;
                    sem.array_end = jsonb_in_array_end;
                    sem.scalar = jsonb_in_scalar;
                    sem.object_field_start = jsonb_in_object_field_start;

                    pg_parse_json_or_ereport(&lex, &sem);
                    freeJsonLexContext(&lex);
                }
                break;

            case JSONTYPE_JSONB:
                // Process existing JSONB value
                {
                    Jsonb *jsonb = DatumGetJsonbP(val);
                    JsonbIterator *it = JsonbIteratorInit(&jsonb->root);

                    if (JB_ROOT_IS_SCALAR(jsonb)) {
                        // Extract scalar value from JSONB wrapper
                        JsonbIteratorNext(&it, &jb, true); // Skip array wrapper
                        JsonbIteratorNext(&it, &jb, true); // Get actual value
                        scalar_jsonb = true;
                    } else {
                        // Copy entire JSONB structure
                        JsonbIteratorToken type;
                        while ((type = JsonbIteratorNext(&it, &jb, false)) != WJB_DONE) {
                            if (type == WJB_END_ARRAY || type == WJB_END_OBJECT ||
                                type == WJB_BEGIN_ARRAY || type == WJB_BEGIN_OBJECT)
                                result->res = pushJsonbValue(&result->parseState, type, NULL);
                            else
                                result->res = pushJsonbValue(&result->parseState, type, &jb);
                        }
                    }
                }
                break;

            default:
                // Convert other types to strings
                outputstr = OidOutputFunctionCall(outfuncoid, val);
                jb.type = jbvString;
                jb.val.string.len = strlen(outputstr);
                checkStringLen(jb.val.string.len, NULL);
                jb.val.string.val = outputstr;
                break;
        }
    }

    // Insert the JsonbValue into the result structure
    if (!is_null && !scalar_jsonb && tcategory >= JSONTYPE_JSON && tcategory <= JSONTYPE_CAST) {
        // Work was done recursively, nothing more needed
        return;
    } else if (result->parseState == NULL) {
        // Single root scalar - wrap in array
        JsonbValue va;
        va.type = jbvArray;
        va.val.array.rawScalar = true;
        va.val.array.nElems = 1;

        result->res = pushJsonbValue(&result->parseState, WJB_BEGIN_ARRAY, &va);
        result->res = pushJsonbValue(&result->parseState, WJB_ELEM, &jb);
        result->res = pushJsonbValue(&result->parseState, WJB_END_ARRAY, NULL);
    } else {
        // Add to existing array or object
        JsonbValue *o = &result->parseState->contVal;
        switch (o->type) {
            case jbvArray:
                result->res = pushJsonbValue(&result->parseState, WJB_ELEM, &jb);
                break;
            case jbvObject:
                result->res = pushJsonbValue(&result->parseState,
                                           key_scalar ? WJB_KEY : WJB_VALUE, &jb);
                break;
            default:
                elog(ERROR, "unexpected parent of nested structure");
        }
    }
}
```