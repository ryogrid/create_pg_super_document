# populate_record_field

## Location
src/backend/utils/adt/jsonfuncs.c: 3404 - 3473

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
  - check_stack_depth
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