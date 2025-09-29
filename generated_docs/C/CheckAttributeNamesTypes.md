CheckAttributeNamesTypes

## Overview
CheckAttributeNamesTypes validates a tuple descriptor to ensure it contains valid attribute names and data types before relation creation, performing comprehensive checks for system conflicts, duplicates, and type validity.

## Definition
void CheckAttributeNamesTypes(TupleDesc tupdesc, char relkind, int flags)

## Detailed Description
CheckAttributeNamesTypes is a critical validation function used during relation creation to ensure the proposed schema is valid. It performs a multi-stage validation process: first checking that the number of attributes does not exceed MaxHeapAttributeNumber, then validating that user-defined attribute names do not conflict with PostgreSQL system attributes (for relations that have system attributes), checking for duplicate attribute names within the schema, and finally validating each attribute type using CheckAttributeType.

The function is designed to catch schema definition errors early and provide clear error messages. It skips system attribute name collision checks for views and composite types since these relation kinds do not have system attributes. The validation is comprehensive and any failure results in an immediate ERROR that aborts the current transaction.

## Parameters / Member Variables
- tupdesc: The tuple descriptor containing attribute definitions to validate
- relkind: The kind of relation being created (table, view, composite type, etc.) which affects which validations are performed
- flags: Control flags passed to CheckAttributeType to specify which data types are allowed

## Dependencies
- Functions called/Symbols referenced:
  - [SystemAttributeByName](../S/SystemAttributeByName.md) (checks for system attribute name conflicts)
  - [CheckAttributeType](CheckAttributeType.md) (validates individual attribute types and properties)
  - TupleDescAttr (macro to access tuple descriptor attributes)
  - NameStr (macro to extract string from Name type)
  - ereport (error reporting)
  - MaxHeapAttributeNumber (constant for maximum allowed attributes)
- Called from (representative examples):
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md) (in src/backend/catalog/heap.c:1151)
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md) (in src/backend/parser/parse_relation.c:1948)

## Notes and Other Information
- Enforces PostgreSQL limit of MaxHeapAttributeNumber columns per table
- System attribute name collision checking is skipped for RELKIND_VIEW and RELKIND_COMPOSITE_TYPE
- Uses O(n²) algorithm for duplicate name detection, but this is acceptable given typical column counts
- Part of the relation creation pipeline, called early to catch schema errors before physical creation
- Error messages are user-friendly and specify the problematic attribute name
- Works in conjunction with CheckAttributeType for complete schema validation
- Located in src/backend/catalog/heap.c:457-518

## Simplified Source

```c
void
CheckAttributeNamesTypes(TupleDesc tupdesc, char relkind, int flags) {
    int i, j;
    int natts = tupdesc->natts;

    // Validate column count doesn't exceed maximum
    if (natts < 0 || natts > MaxHeapAttributeNumber)
        ereport(ERROR, "tables can have at most %d columns", MaxHeapAttributeNumber);

    // Check for conflicts with system attribute names
    // Skip this for views and composite types which don't have system attributes
    if (relkind != RELKIND_VIEW && relkind != RELKIND_COMPOSITE_TYPE) {
        for (i = 0; i < natts; i++) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

            if (SystemAttributeByName(NameStr(attr->attname)) != NULL)
                ereport(ERROR, "column name \"%s\" conflicts with a system column name",
                        NameStr(attr->attname));
        }
    }

    // Check for duplicate attribute names
    for (i = 1; i < natts; i++) {
        for (j = 0; j < i; j++) {
            if (strcmp(NameStr(TupleDescAttr(tupdesc, j)->attname),
                      NameStr(TupleDescAttr(tupdesc, i)->attname)) == 0)
                ereport(ERROR, "column name \"%s\" specified more than once",
                        NameStr(TupleDescAttr(tupdesc, j)->attname));
        }
    }

    // Validate each attribute type
    for (i = 0; i < natts; i++) {
        CheckAttributeType(NameStr(TupleDescAttr(tupdesc, i)->attname),
                          TupleDescAttr(tupdesc, i)->atttypid,
                          TupleDescAttr(tupdesc, i)->attcollation,
                          NIL, /* assume we're creating a new rowtype */
                          flags);
    }
}
```