# CreateInheritance

## Location
[src/backend/commands/tablecmds.c:15773-15841](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L15773-L15841)

## Overview
CreateInheritance is a static function that handles the catalog manipulation portion of creating inheritance relationships between a child table and a parent table in PostgreSQL.

## Definition

```c
static void
CreateInheritance(Relation child_rel, Relation parent_rel, bool ispartition)
```
## Detailed Description
This function performs the core catalog operations needed to establish inheritance between two relations. It validates the inheritance relationship, ensures no duplicate inheritance exists, merges attributes and constraints between parent and child tables, and creates the necessary catalog entries in pg_inherits. The function is common to both ATExecAddInherit() (for ALTER TABLE INHERIT) and ATExecAttachPartition() (for partition attachment operations).

The function performs several key steps:
1. Opens the pg_inherits catalog with RowExclusiveLock
2. Scans existing inheritance relationships to detect duplicates and determine the next sequence number
3. Calls MergeAttributesIntoExisting to handle attribute inheritance and increment attinhcount
4. Calls MergeConstraintsIntoExisting to handle constraint inheritance and increment coninhcount  
5. Creates the inheritance catalog entry via StoreCatalogInheritance1
6. Closes the catalog relation

## Parameters / Member Variables
- : The child relation that will inherit from the parent
- : The parent relation to inherit from
- : Boolean flag indicating whether this is a partition relationship (affects attribute merging behavior)

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [MergeAttributesIntoExisting](../M/MergeAttributesIntoExisting.md)
  - [MergeConstraintsIntoExisting](../M/MergeConstraintsIntoExisting.md)
  - [StoreCatalogInheritance1](../S/StoreCatalogInheritance1.md)
  - table_close
  - Form_pg_inherits
  - [SysScanDesc](../S/SysScanDesc.md)
- Called from (representative examples):
  - [ATExecAddInherit](../A/ATExecAddInherit.md)
  - [ATExecAttachPartition](../A/ATExecAttachPartition.md)

## Notes and Other Information
- The function acquires RowExclusiveLock on the pg_inherits catalog to ensure safe concurrent access
- Inheritance sequence numbers (inhseqno) start at 1 and are incremented for each new parent relationship
- The function validates against duplicate inheritance relationships by checking if the child already inherits from the same parent
- For partition relationships, additional validation ensures the child is not already inheriting from other relations
- The function does not reject cases where indirect inheritance already exists, consistent with CREATE TABLE behavior