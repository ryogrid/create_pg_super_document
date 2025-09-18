# ReorderBufferTupleCidEnt

## Location
src/backend/replication/logical/reorderbuffer.c: 141 - 147

## Overview
ReorderBufferTupleCidEnt is a hash table entry structure that stores command ID mapping information for specific tuples, containing both the tuple location key and the associated command IDs (cmin, cmax, combocid).

## Definition


## Detailed Description
This structure serves as a complete hash table entry that maps tuple locations to their command ID information in PostgreSQL's logical replication system. It combines the ReorderBufferTupleCidKey (which identifies the tuple location) with the actual command ID values that determine when the tuple was created (cmin) and when it was last modified or deleted (cmax). The combocid field is included for debugging purposes and helps track combination command IDs in complex transaction scenarios. This mapping is essential for logical decoding to determine tuple visibility and maintain transactional consistency.

## Parameters / Member Variables
- : ReorderBufferTupleCidKey structure containing the relation file locator and tuple ID that uniquely identifies the tuple location
- : CommandId indicating when the tuple was created within its transaction
- : CommandId indicating when the tuple was last modified or marked for deletion within its transaction
- : CommandId used for debugging purposes to track combination command IDs in complex transaction scenarios

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTupleCidKey](ReorderBufferTupleCidKey.md) (tuple location key structure)
  - CommandId (PostgreSQL command ID type)
- Called from (representative examples):
  - [ReorderBufferBuildTupleCidHash](ReorderBufferBuildTupleCidHash.md) (at src/backend/replication/logical/reorderbuffer.c:1787, 1801, 1817)
  - [DisplayMapping](../D/DisplayMapping.md) (at src/backend/replication/logical/reorderbuffer.c:5186, 5189)
  - [ApplyLogicalMappingFile](../A/ApplyLogicalMappingFile.md) (at src/backend/replication/logical/reorderbuffer.c:5228, 5229, 5259, 5270)
  - [ResolveCminCmaxDuringDecoding](ResolveCminCmaxDuringDecoding.md) (at src/backend/replication/logical/reorderbuffer.c:5410, 5445)

## Notes and Other Information
This structure is the complete entry type used in hash tables that implement (relfilelocator, ctid) => (cmin, cmax) mappings. The command ID information stored in these entries is crucial for logical replication's ability to properly decode WAL records and determine which version of a tuple should be visible to subscribers. The combocid field is particularly useful during development and debugging to understand how complex transaction scenarios involving multiple commands are being handled.