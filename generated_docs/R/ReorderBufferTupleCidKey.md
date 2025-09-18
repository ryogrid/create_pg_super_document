# ReorderBufferTupleCidKey

## Location
src/backend/replication/logical/reorderbuffer.c: 135 - 139

## Overview
ReorderBufferTupleCidKey is a composite key structure used in hash tables to map tuple locations (defined by relation file locator and tuple ID) to their command ID information during logical replication decoding.

## Definition


## Detailed Description
This structure serves as the key component for hash tables that maintain mappings between specific tuple locations and their associated command IDs (cmin, cmax). In PostgreSQL's logical replication system, it's essential to track when tuples were created and modified within transactions to properly decode WAL records and maintain consistency. The combination of relation file locator and tuple ID uniquely identifies a tuple's physical location, making it an ideal composite key for such mappings.

## Parameters / Member Variables
- : RelFileLocator structure that identifies the specific relation file (tablespace, database, relation)
- : ItemPointerData structure containing the block number and offset that pinpoint the exact tuple location within the relation

## Dependencies
- Functions called/Symbols referenced:
  - RelFileLocator (PostgreSQL relation file identification structure)
  - ItemPointerData (PostgreSQL tuple ID structure)
- Called from (representative examples):
  - ReorderBufferTupleCidEnt (at src/backend/replication/logical/reorderbuffer.c:143)
  - ReorderBufferBuildTupleCidHash (at src/backend/replication/logical/reorderbuffer.c:1786, 1800, 1810)
  - ApplyLogicalMappingFile (at src/backend/replication/logical/reorderbuffer.c:5227, 5233)
  - ResolveCminCmaxDuringDecoding (at src/backend/replication/logical/reorderbuffer.c:5409)

## Notes and Other Information
This key structure is specifically designed for PostgreSQL's hash table implementation and is used to create (relfilelocator, ctid) => (cmin, cmax) mappings. The structure enables efficient lookup of command ID information for specific tuples during logical decoding, which is crucial for determining tuple visibility and maintaining transactional consistency in logical replication scenarios.