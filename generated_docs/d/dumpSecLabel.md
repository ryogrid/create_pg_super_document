# dumpSecLabel

## Location
[src/bin/pg_dump/pg_dump.c:15390-15469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L15390-L15469)

## Overview
Generates SECURITY LABEL statements for database objects that have security labels applied through external security label providers.

## Definition


## Detailed Description
The  function handles the dumping of security labels for database objects. Security labels are a PostgreSQL feature that allows external security modules (such as SELinux, AppArmor, or custom security providers) to attach additional security context information to database objects.

Key functionality includes:
1. **Provider-specific labeling** - Supports multiple security label providers, each with their own namespace
2. **Object type differentiation** - Handles different treatment for regular objects vs. large objects (large object labels are considered data)
3. **Sub-object support** - Uses subid parameter to handle labels on specific parts of objects (e.g., table columns)
4. **Option compliance** - Respects --no-security-labels and schema/data-only dump options
5. **Dependency management** - Creates proper dependencies to ensure labels are applied after objects exist

The function queries the internal security label storage using  and generates SECURITY LABEL FOR statements for each applicable label.

## Parameters / Member Variables
- : Archive structure for output generation and configuration
- : Database object type string (TABLE, FUNCTION, etc.)
- : Properly formatted object name (already quoted if necessary)
- : Schema/namespace name for schema decoration, or NULL
- : Object owner for archive entry metadata
- : System catalog identifiers used to look up security labels in pg_seclabel
- : Sub-object identifier for targeting specific parts of objects (0 for main object)
- : Dump ID of the associated object for dependency tracking

## Dependencies
- Functions called/Symbols referenced:
  - [findSecLabels](../f/findSecLabels.md)
  - createPQExpBuffer
  - [fmtId](../f/fmtId.md)
  - appendStringLiteralAH
  - [createDumpId](../c/createDumpId.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - destroyPQExpBuffer
- Called from (representative examples):
  - [dumpFunc](dumpFunc.md)
  - [dumpNamespace](dumpNamespace.md)
  - [dumpEnumType](dumpEnumType.md)
  - [dumpLO](dumpLO.md)
  - [dumpPublication](dumpPublication.md)

## Notes and Other Information
- The function is designed to be called after  for the main object, as dependency sorting has already occurred
- Security labels are treated as schema information except for large objects, where they are considered data
- In binary upgrade mode, large object security labels are included even in schema-only dumps
- The function creates archive entries in SECTION_NONE, allowing flexibility in when labels are applied during restoration
- Multiple security label providers can label the same object, each generating separate SECURITY LABEL statements
- The subid parameter allows fine-grained labeling of object components (though this is primarily used for future extensibility)
- Labels are properly escaped and quoted using  to handle special characters safely