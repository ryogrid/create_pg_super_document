# binary_upgrade_set_type_oids_by_rel

## Location
[src/bin/pg_dump/pg_dump.c:5461-5472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L5461-L5472)

## Overview
A convenience wrapper function that preserves type OIDs for table-related composite types during PostgreSQL binary upgrades.

## Definition
```c
static void binary_upgrade_set_type_oids_by_rel(Archive *fout,
                                                PQExpBuffer upgrade_buffer,
                                                const TableInfo *tbinfo)
```

## Detailed Description
This function is a specialized wrapper around `binary_upgrade_set_type_oids_by_type_oid` specifically designed for handling composite types associated with database tables. In PostgreSQL, every table has an associated composite type that represents the row structure. During binary upgrades, these composite type OIDs must be preserved to maintain consistency.

The function extracts the composite type OID from the table's metadata (`tbinfo->reltype`) and delegates the actual OID preservation work to `binary_upgrade_set_type_oids_by_type_oid`. It passes `false` for both `force_array_type` and `include_multirange_type` parameters since table composite types typically don't require array types or multirange type handling.

## Parameters / Member Variables
- `fout`: Archive structure containing database connection and dump context information
- `upgrade_buffer`: PQExpBuffer where the generated binary upgrade SQL statements will be appended
- `tbinfo`: TableInfo structure containing metadata about the table, including its associated composite type OID

## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid (macro to check if OID is valid)
  - [binary_upgrade_set_type_oids_by_type_oid](binary_upgrade_set_type_oids_by_type_oid.md) (core function for type OID preservation)
  - [TableInfo](../T/TableInfo.md) (struct containing table metadata)
- Called from (representative examples):
  - [dumpTableSchema](../d/dumpTableSchema.md) (src/bin/pg_dump/pg_dump.c:15973)
  - fmtQualifiedDumpable (src/bin/pg_dump/pg_dump.c:323)

## Notes and Other Information
- This is a thin wrapper that simplifies the interface for table-specific type OID preservation
- Only operates when the table has a valid associated composite type (OidIsValid check)
- Uses conservative parameters for `binary_upgrade_set_type_oids_by_type_oid`: no forced array types or multirange types
- Part of the broader binary upgrade infrastructure that ensures object identity preservation across PostgreSQL major version upgrades
- The reltype field in TableInfo corresponds to the OID of the composite type in pg_type that represents the table's row structure
- Essential for maintaining referential integrity in systems that rely on specific type OIDs for table row types

## Simplified Source

```c
static void binary_upgrade_set_type_oids_by_rel(Archive *fout,
                                                PQExpBuffer upgrade_buffer,
                                                const TableInfo *tbinfo)
{
    Oid type_oid = tbinfo->reltype;

    // Only process if table has an associated composite type
    if (OidIsValid(type_oid)) {
        // Delegate to main type OID preservation function
        // Use conservative settings: no forced arrays, no multirange types
        binary_upgrade_set_type_oids_by_type_oid(fout, upgrade_buffer,
                                                  type_oid, false, false);
    }
}
```