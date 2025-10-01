# prepare_column_cache

## Location
[src/backend/utils/adt/jsonfuncs.c:3249-3342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3249-L3342)

## Overview
Initializes and configures column metadata cache for a specific PostgreSQL data type, setting up appropriate I/O information and type categorization for JSON processing operations.

## Definition

```c
static void
prepare_column_cache(ColumnIOData *column,
					 Oid typid,
					 int32 typmod,
					 MemoryContext mcxt,
					 bool need_scalar)
```
## Detailed Description
This function prepares column metadata cache by analyzing the given type ID and configuring the ColumnIOData structure accordingly. It categorizes types into different categories (TYPECAT_DOMAIN, TYPECAT_COMPOSITE, TYPECAT_ARRAY, TYPECAT_SCALAR) and sets up the appropriate I/O information for each category. The function handles various PostgreSQL type system complexities including domains, composite types, arrays, and scalar types. For domains, it resolves to the base type while preserving domain constraint information. For composite types and records, it sets up record I/O structures. For arrays, it configures element type information. For scalar types or when explicitly requested, it sets up type input/output function information.

## Parameters / Member Variables
- : Pointer to ColumnIOData structure to be initialized with type metadata
- : PostgreSQL type OID identifying the specific data type
- : Type modifier providing additional type-specific information
- : Memory context for allocating persistent type information structures
- : Boolean flag forcing scalar I/O info lookup even for non-scalar types

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md)
  - [get_typtype](../g/get_typtype.md)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - IsTrueArrayType
  - [getTypeInputInfo](../g/getTypeInputInfo.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - JsObjectFree
  - [populate_record_field](populate_record_field.md)
  - [get_record_type_from_argument](../g/get_record_type_from_argument.md)

## Notes and Other Information
This function is static and specifically designed for JSON processing functionality in PostgreSQL. It performs comprehensive type analysis to determine the most efficient processing strategy for different PostgreSQL data types. The function handles the complexity of PostgreSQL's type system including domain types (which can be domains over other domains or composites), composite types, array types, and scalar types. Memory allocation is performed in the provided memory context to ensure proper cleanup. The function is critical for the JSON populate functions to correctly handle diverse PostgreSQL data types.

## Simplified Source

```c
static void prepare_column_cache(ColumnIOData *column, Oid typid,
                                int32 typmod, MemoryContext mcxt,
                                bool need_scalar) {
    HeapTuple tup;
    Form_pg_type type;

    column->typid = typid;
    column->typmod = typmod;

    // Look up type information in system catalog
    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for type %u", typid);

    type = (Form_pg_type) GETSTRUCT(tup);

    // Categorize type and set up appropriate I/O structures
    if (type->typtype == TYPTYPE_DOMAIN) {
        // Handle domain types
        Oid base_typid;
        int32 base_typmod = typmod;

        base_typid = getBaseTypeAndTypmod(typid, &base_typmod);

        if (get_typtype(base_typid) == TYPTYPE_COMPOSITE) {
            // Domain over composite type
            column->typcat = TYPECAT_COMPOSITE_DOMAIN;
            column->io.composite.record_io = NULL;
            column->io.composite.tupdesc = NULL;
            column->io.composite.base_typid = base_typid;
            column->io.composite.base_typmod = base_typmod;
            column->io.composite.domain_info = NULL;
        } else {
            // Domain over other type
            column->typcat = TYPECAT_DOMAIN;
            column->io.domain.base_typid = base_typid;
            column->io.domain.base_typmod = base_typmod;
            column->io.domain.base_io = MemoryContextAllocZero(mcxt, sizeof(ColumnIOData));
            column->io.domain.domain_info = NULL;
        }
    } else if (type->typtype == TYPTYPE_COMPOSITE || typid == RECORDOID) {
        // Handle composite/record types
        column->typcat = TYPECAT_COMPOSITE;
        column->io.composite.record_io = NULL;
        column->io.composite.tupdesc = NULL;
        column->io.composite.base_typid = typid;
        column->io.composite.base_typmod = typmod;
        column->io.composite.domain_info = NULL;
    } else if (IsTrueArrayType(type)) {
        // Handle array types
        column->typcat = TYPECAT_ARRAY;
        column->io.array.element_info = MemoryContextAllocZero(mcxt, sizeof(ColumnIOData));
        column->io.array.element_type = type->typelem;
        column->io.array.element_typmod = typmod;
    } else {
        // Handle scalar types
        column->typcat = TYPECAT_SCALAR;
        need_scalar = true;
    }

    // Set up scalar I/O info if needed
    if (need_scalar) {
        Oid typioproc;
        getTypeInputInfo(typid, &typioproc, &column->scalar_io.typioparam);
        fmgr_info_cxt(typioproc, &column->scalar_io.typiofunc, mcxt);
    }

    ReleaseSysCache(tup);
}
```