# SecLabelItem

## Location
[src/bin/pg_dump/pg_dump.c:94-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L94-L95)

## Overview
SecLabelItem is a structure used in pg_dump to represent security labels associated with database objects, storing the label provider, label text, and object identification information for security label management during database dumps.

## Definition


## Detailed Description
SecLabelItem is a data structure used exclusively in pg_dump to manage security labels during database backup operations. Security labels are used by security-enhanced PostgreSQL installations to store security context information (such as SELinux labels) for database objects. This structure provides a way to organize and access security label information efficiently during the dump process.

The structure is used to build a sorted array of security labels that can be quickly searched when dumping objects. The pg_dump utility collects all security labels from the database and stores them in this format to enable efficient lookup when generating dump output for specific objects.

## Parameters / Member Variables
- : A string identifying the security label provider (e.g., "selinux", "dummy")
- : The actual security label text assigned to the object
- : The OID of the system catalog that contains the object (e.g., RelationRelationId for tables)
- : The OID of the specific object within its catalog
- : Sub-object identifier, typically used for table columns (0 for the object itself, >0 for specific columns)

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [findSecLabels](../f/findSecLabels.md) (searches the seclabels array)
  - [dumpSecLabel](../d/dumpSecLabel.md) (dumps security labels for objects)
  - [dumpTableSecLabel](../d/dumpTableSecLabel.md) (dumps security labels for table columns)
  - [collectSecLabels](../c/collectSecLabels.md) (collects security labels from the database)

## Notes and Other Information
- Used only in pg_dump utility, not in the core PostgreSQL server
- The global static array  stores all SecLabelItem instances
- Security labels are an optional feature and may not be present in all PostgreSQL installations
- The structure supports hierarchical objects through the objsubid field, allowing labels on both tables and their individual columns
- Security labels are typically used in high-security environments with mandatory access control systems