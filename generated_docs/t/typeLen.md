# typeLen

## Location
[src/backend/parser/parse_type.c:599-608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L599-L608)

## Overview
typeLen extracts the length field from a Type structure, returning the storage length of the PostgreSQL data type.

## Definition
```c
int16 typeLen(Type t)
```

## Detailed Description
This function is a simple accessor that retrieves the typlen field from a Type structure. The typlen field indicates the storage length of the data type: positive values represent fixed-length types (number of bytes), -1 indicates variable-length types (varlena), and -2 indicates null-terminated C strings.

The function extracts the Form_pg_type structure from the HeapTuple and returns the typlen field directly.

## Parameters / Member Variables
- `t`: Type structure (HeapTuple) containing pg_type catalog data

## Dependencies
- Functions called/Symbols referenced:
  - Type (parameter type)
  - Form_pg_type (for accessing tuple structure)
  - GETSTRUCT (macro to extract structure from tuple)
- Called from (representative examples):
  - [coerce_type](../c/coerce_type.md)

## Notes and Other Information
- Returns the typlen field which has special meaning in PostgreSQL:
  - Positive values: fixed-length types (e.g., 4 for int4, 8 for int8)
  - -1: variable-length types (varlena types like text, varchar)
  - -2: null-terminated C strings (like cstring)
- Simple accessor function used primarily in type coercion and validation
- No error checking on input parameter - assumes valid Type structure
- Located in src/backend/parser/parse_type.c:599-608
- Essential for determining memory allocation and storage requirements for type values

## Simplified Source

```c
int16 typeLen(Type t) {
    // Extract the type structure from the heap tuple
    Form_pg_type typ = (Form_pg_type) GETSTRUCT(t);

    // Return the type length field
    // Positive: fixed-length types (bytes), -1: variable-length, -2: C strings
    return typ->typlen;
}
```

**Simplification Notes:**
- Added explanatory comments describing the typlen semantics
- Function is already minimal, so only added documentation
- Core logic: extract type structure and return the length field
- Preserved the essential purpose: provide storage length information for the type