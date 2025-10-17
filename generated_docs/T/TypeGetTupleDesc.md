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
  - [list_length](../l/list_length.md)
  - [list_nth](../l/list_nth.md)
  - strVal
  - linitial
  - TupleDescAttr
  - [namestrcpy](../n/namestrcpy.md)
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

## Simplified Source

```c
TupleDesc TypeGetTupleDesc(Oid typeoid, List *colaliases) {
    Oid base_typeoid;
    TypeFuncClass functypclass = get_type_func_class(typeoid, &base_typeoid);
    TupleDesc tupdesc = NULL;

    if (functypclass == TYPEFUNC_COMPOSITE) {
        // Handle composite types (table row types)
        tupdesc = lookup_rowtype_tupdesc_copy(base_typeoid, -1);

        if (colaliases != NIL) {
            int natts = tupdesc->natts;

            // Validate alias count matches attribute count
            if (list_length(colaliases) != natts)
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("number of aliases does not match number of columns")));

            // Apply column aliases
            for (int varattno = 0; varattno < natts; varattno++) {
                char *label = strVal(list_nth(colaliases, varattno));
                Form_pg_attribute attr = TupleDescAttr(tupdesc, varattno);

                if (label != NULL)
                    namestrcpy(&(attr->attname), label);
            }

            // Convert to anonymous record type
            tupdesc->tdtypeid = RECORDOID;
            tupdesc->tdtypmod = -1;
        }
    }
    else if (functypclass == TYPEFUNC_SCALAR) {
        // Handle scalar types - require exactly one alias
        if (colaliases == NIL)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("no column alias was provided")));

        if (list_length(colaliases) != 1)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("number of aliases does not match number of columns")));

        char *attname = strVal(linitial(colaliases));

        // Create single-column tuple descriptor
        tupdesc = CreateTemplateTupleDesc(1);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, attname, typeoid, -1, 0);
    }
    else if (functypclass == TYPEFUNC_RECORD) {
        // Cannot support RECORD types without typmod
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("could not determine row description for function returning record")));
    }
    else {
        elog(ERROR, "function in FROM has unsupported return type");
    }

    return tupdesc;
}
```