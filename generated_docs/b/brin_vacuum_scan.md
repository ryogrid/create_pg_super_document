# brin_vacuum_scan

## Location
src/backend/access/brin/brin.c: 2163 - 2195

## Overview
Performs a complete physical scan of a BRIN index during VACUUM operations to identify and clean up uncataloged index pages that may have been lost due to crashes or other failures.

## Definition
static void brin_vacuum_scan(Relation idxrel, BufferAccessStrategy strategy)

## Detailed Description
This function implements a comprehensive maintenance scan of a BRIN index as part of the VACUUM process. It performs a complete physical traversal of all index pages to detect and resolve various issues that may arise from system failures, crashes, or incomplete operations.

The function operates in two main phases:
1. **Page-by-page cleanup**: Scans every block in the index in physical order, applying cleanup operations to each page through brin_page_cleanup()
2. **Free Space Map maintenance**: Updates the entire Free Space Map (FSM) hierarchy to ensure accurate free space tracking and repair any existing damage

This scanning approach is designed to be resilient against index corruption and ensures that the index remains in a consistent state even after system failures. The function uses the provided buffer access strategy to manage memory efficiently during the scan.

## Parameters / Member Variables
- : The BRIN index relation being vacuumed, containing metadata about the index structure and storage
- : BufferAccessStrategy that controls buffer management behavior during the scan, allowing for memory-efficient processing of large indexes

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfBlocks: Retrieves the total number of blocks in the index relation
  - [ReadBufferExtended](../R/ReadBufferExtended.md): Reads index blocks with specified access strategy and options
  - [brin_page_cleanup](brin_page_cleanup.md): Performs cleanup operations on individual BRIN index pages
  - ReleaseBuffer: Releases buffer locks after processing each page
  - FreeSpaceMapVacuum: Updates and repairs the Free Space Map for the entire index
  - CHECK_FOR_INTERRUPTS: Allows cancellation during long-running operations
  - MAIN_FORKNUM, RBM_NORMAL: Constants for fork identification and buffer read mode

- Called from (representative examples):
  - [brinvacuumcleanup](brinvacuumcleanup.md): Main BRIN index vacuum cleanup routine

## Notes and Other Information
- This is a static function, only accessible within the brin.c file
- Scans the index in physical block order rather than logical order for efficiency
- Includes interrupt checking (CHECK_FOR_INTERRUPTS) to allow for cancellation during long operations
- The FSM vacuum at the end ensures that free space information is accurate across the entire index
- Part of PostgreSQL's index maintenance infrastructure, specifically designed for BRIN index recovery
- Uses the provided BufferAccessStrategy to avoid overwhelming the shared buffer pool during large index scans
- Critical for maintaining BRIN index consistency after system failures or incomplete operations
- The function is designed to be safe to run on potentially corrupted or inconsistent indexes