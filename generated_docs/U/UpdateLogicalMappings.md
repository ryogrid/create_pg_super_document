# UpdateLogicalMappings

## Location
src/backend/replication/logical/reorderbuffer.c: 5326 - 5403

## Overview
UpdateLogicalMappings applies existing logical remapping files that are targeted at a specific transaction and relation during logical replication decoding.

## Definition


## Detailed Description
UpdateLogicalMappings is responsible for discovering and applying logical tuple remapping files that were created during heap rewrite operations (such as CLUSTER, VACUUM FULL, or ALTER TABLE operations that require table rewrites). These mapping files contain information about how tuple identities changed during the rewrite process, which is essential for logical replication to correctly track changes.

The function performs the following key operations:
1. Scans the pg_logical/mappings directory for relevant mapping files
2. Filters files based on database OID, relation OID, and transaction visibility
3. Validates that the creating transaction committed
4. Sorts applicable files by LSN (Log Sequence Number) to ensure correct ordering
5. Applies each relevant mapping file through ApplyLogicalMappingFile

The mapping files follow the LOGICAL_REWRITE_FORMAT naming convention and contain metadata about which transactions and relations they apply to, along with LSN information for proper sequencing.

## Parameters / Member Variables
- : Hash table storing tuple command ID data that will be updated with the mapping information
- : Object identifier of the relation for which mappings should be applied
- : Snapshot containing transaction visibility information, including the array of subtransaction IDs

## Dependencies
- Functions called/Symbols referenced:
  - IsSharedRelation
  - AllocateDir
  - ReadDir
  - FreeDir
  - TransactionIdDidCommit
  - TransactionIdInArray
  - list_sort
  - file_sort_by_lsn
  - ApplyLogicalMappingFile
  - RewriteMappingFile
  - LOGICAL_REWRITE_FORMAT
- Called from (representative examples):
  - ResolveCminCmaxDuringDecoding

## Notes and Other Information
- This function is static and only used within the reorderbuffer.c module
- The function handles both regular and shared relations by setting dboid appropriately
- Files are processed in LSN order to maintain consistency with the logical replication timeline
- The function includes extensive validation to ensure only relevant and valid mapping files are processed
- Mapping files that correspond to aborted transactions are ignored
- The function is critical for maintaining data consistency during logical replication when heap rewrites occur