# TypeGetTupleDesc

## Location
[src/backend/utils/fmgr/funcapi.c:1903-2004](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L1903-L2004)

## Overview
Constructs a tuple descriptor from a type OID, supporting composite and scalar types with optional column aliasing for legacy compatibility.

## Definition
```c
TupleDesc TypeGetTupleDesc(Oid typeoid, List *colaliases)
```

## Detailed Description
This function builds a TupleDesc from a given type OID, with different behavior depending on the type class (composite, scalar, or record). It's primarily maintained for backwards compatibility, as modern code should use get_call_result_type or related functions that better handle OUT parameters, RECORD types, and polymorphic results.

For composite types, it retrieves the existing tuple descriptor and optionally applies column aliases if provided. For scalar types, it creates a single-column tuple descriptor using a required alias. The function does not support TYPEFUNC_COMPOSITE_DOMAIN to avoid complexity with domain constraints that legacy callers might not handle properly.

The function determines the type class using get_type_func_class and handles each case appropriately, with specific error handling for unsupported scenarios like RECORD types without typmod information.

## Parameters / Member Variables
- `typeoid`: The OID of the data type for which to build a tuple descriptor
- `colaliases`: A list of column aliases; required for scalar types (must have exactly 1 element), optional for composite types (must match column count if provided)

## Dependencies
- Functions called/Symbols referenced:
  - [get_type_func_class](../g/get_type_func_class.md)
  - [lookup_rowtype_tupdesc_copy](../l/lookup_rowtype_tupdesc_copy.md)
  - list_length
  - [list_nth](../l/list_nth.md)
  - strVal
  - linitial
  - TupleDescAttr
  - namestrcpy
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitEntry](TupleDescInitEntry.md)
  - ereport, errcode, errmsg
  - TYPEFUNC_COMPOSITE, TYPEFUNC_SCALAR, TYPEFUNC_RECORD
- Called from (representative examples):
  - TypeFuncClass

## Notes and Other Information
- Deprecated usage: modern code should prefer get_call_result_type and related functions
- Does not support TYPEFUNC_COMPOSITE_DOMAIN to avoid domain constraint complications
- For composite types with aliases, creates an anonymous RECORD type with modified column names
- Requires exactly one alias for scalar types, optional aliases for composite types
- Cannot handle RECORD types due to lack of typmod parameter
- Column alias count must match the number of attributes in composite types
- Returns a newly allocated tuple descriptor that the caller must manage