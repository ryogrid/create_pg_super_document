# populate_record_field

## Location
[src/backend/utils/adt/jsonfuncs.c:3404-3473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3404-L3473)

## Overview
Recursively populates a PostgreSQL record field or array element from a JSON/JSONB value, handling type conversion based on the target type's category (scalar, array, composite, domain).

## Definition
```c
static Datum populate_record_field(ColumnIOData *col,
                                   Oid typid,
                                   int32 typmod,
                                   const char *colname,
                                   MemoryContext mcxt,
                                   Datum defaultval,
                                   JsValue *jsv,
                                   bool *isnull,
                                   Node *escontext,
                                   bool omit_scalar_quotes)
```

## Detailed Description
This function serves as the central dispatcher for converting JSON/JSONB values to PostgreSQL data types. It first ensures the column metadata cache is prepared for the target type, then determines the appropriate conversion strategy based on the type category. The function handles special cases like converting JSON strings to complex types through input functions and ensures proper domain constraint checking for null values. It dispatches to specialized population functions (populate_scalar, populate_array, populate_composite, populate_domain) based on the determined type category.

## Parameters / Member Variables
- `col`: Column metadata cache containing type information and I/O functions
- `typid`: Target PostgreSQL type OID for the conversion
- `typmod`: Type modifier providing additional type-specific constraints
- `colname`: Name of the column being populated (for error reporting)
- `mcxt`: Memory context for temporary allocations during conversion
- `defaultval`: Default value to use for composite types
- `jsv`: JSON/JSONB value structure containing the source data
- `isnull`: Pointer to null indicator flag, set if source is null
- `escontext`: Error context for soft error handling
- `omit_scalar_quotes`: Flag to strip quotes from scalar string conversions

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - [prepare_column_cache](prepare_column_cache.md)
  - JsValueIsNull
  - JsValueIsString
  - [populate_scalar](populate_scalar.md)
  - [populate_array](populate_array.md)
  - [populate_composite](populate_composite.md)
  - [populate_domain](populate_domain.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - DatumGetHeapTupleHeader
- Called from (representative examples):
  - JsObjectFree
  - [populate_array_element](populate_array_element.md)
  - [populate_domain](populate_domain.md)
  - [json_populate_type](../j/json_populate_type.md)
  - [populate_record](populate_record.md)

## Notes and Other Information
This function is the core of PostgreSQL's JSON-to-PostgreSQL type conversion system. It implements a recursive approach that can handle arbitrarily nested structures. The function includes an important optimization where JSON strings can be converted to complex types (arrays, composites) through their text input functions, providing flexibility in JSON structure handling. Stack depth checking prevents infinite recursion in pathological cases. The function properly handles PostgreSQL's domain types, which require constraint checking even for null values. Error context support allows for graceful error handling in contexts where exceptions are not appropriate.

## Simplified Source

```c
static Datum populate_record_field(ColumnIOData *col, Oid typid, int32 typmod,
                                   const char *colname, MemoryContext mcxt,
                                   Datum defaultval, JsValue *jsv, bool *isnull,
                                   Node *escontext, bool omit_scalar_quotes) {
    TypeCat typcat;

    // Prevent stack overflow in recursive calls
    check_stack_depth();

    // Prepare column metadata cache if type changed
    if (col->typid != typid || col->typmod != typmod)
        prepare_column_cache(col, typid, typmod, mcxt, true);

    *isnull = JsValueIsNull(jsv);
    typcat = col->typcat;

    // Convert JSON strings to complex types via input function
    if (JsValueIsString(jsv) &&
        (typcat == TYPECAT_ARRAY ||
         typcat == TYPECAT_COMPOSITE ||
         typcat == TYPECAT_COMPOSITE_DOMAIN))
        typcat = TYPECAT_SCALAR;

    // Handle nulls (domains still need constraint checking)
    if (*isnull && typcat != TYPECAT_DOMAIN && typcat != TYPECAT_COMPOSITE_DOMAIN)
        return (Datum) 0;

    // Dispatch to appropriate type-specific population function
    switch (typcat) {
        case TYPECAT_SCALAR:
            return populate_scalar(&col->scalar_io, typid, typmod, jsv,
                                 isnull, escontext, omit_scalar_quotes);

        case TYPECAT_ARRAY:
            return populate_array(&col->io.array, colname, mcxt, jsv,
                                isnull, escontext);

        case TYPECAT_COMPOSITE:
        case TYPECAT_COMPOSITE_DOMAIN:
            return populate_composite(&col->io.composite, typid, colname, mcxt,
                                    DatumGetPointer(defaultval) ?
                                    DatumGetHeapTupleHeader(defaultval) : NULL,
                                    jsv, isnull, escontext);

        case TYPECAT_DOMAIN:
            return populate_domain(&col->io.domain, typid, colname, mcxt,
                                 jsv, isnull, escontext, omit_scalar_quotes);

        default:
            elog(ERROR, "unrecognized type category '%c'", typcat);
            return (Datum) 0;
    }
}
```