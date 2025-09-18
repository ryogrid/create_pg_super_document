# rf_context

## Location
src/backend/commands/publicationcmds.c: 55 - 62

## Overview
A context structure used to validate columns in row filter expressions for PostgreSQL logical replication publications, ensuring that filtered columns are part of the relation's replica identity.

## Definition


## Detailed Description
The  structure serves as a validation context for row filter expressions in PostgreSQL's logical replication publication system. It is specifically designed to ensure that all columns referenced in a publication's row filter WHERE clause are part of the relation's REPLICA IDENTITY. This validation is critical for logical replication consistency, as only replica identity columns can be safely used for filtering changes that will be replicated to subscribers.

The struct is used during row filter expression tree walking to maintain state about which columns are valid for filtering and to handle the complexities of partition inheritance where parent and child tables may have different column orderings.

## Parameters / Member Variables
- : A bitmapset containing the attribute numbers of columns that are part of the relation's replica identity index, used to validate that filter columns are replication-safe
- : Boolean flag indicating whether the validation is being performed on a parent relation's row filter that will be applied to partition changes (affects column number translation)
- : Object identifier of the actual relation being validated (typically a partition when pubviaroot is true)  
- : Object identifier of the parent relation whose row filter is being validated (used for column name/number mapping in partition scenarios)

## Dependencies
- Functions called/Symbols referenced:
  - [Bitmapset](../B/Bitmapset.md) (bitmap operations)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [contain_invalid_rfcolumn_walker](../c/contain_invalid_rfcolumn_walker.md)
  - [pub_rf_contains_invalid_column](../p/pub_rf_contains_invalid_column.md)

## Notes and Other Information
This structure is integral to PostgreSQL's logical replication row filtering feature introduced to allow publications to filter which row changes are replicated. The validation ensures data consistency by preventing the use of non-replica-identity columns in filters, which could lead to replication conflicts or data inconsistencies on subscriber nodes. The pubviaroot mechanism handles the complexity of partitioned tables where a parent table's row filter may be applied to partition changes, requiring careful column mapping between parent and child relations.