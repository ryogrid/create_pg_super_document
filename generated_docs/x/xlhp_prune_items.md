# xlhp_prune_items

## Location
[src/include/access/heapam_xlog.h:377-381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/heapam_xlog.h#L377-L381)

## Overview
A generic sub-record type used in heap pruning WAL records to store information about redirect, dead, and unused items during heap page pruning operations.

## Definition


## Detailed Description
The  struct is a specialized data structure used within PostgreSQL's Write-Ahead Logging (WAL) system for heap pruning operations. It serves as a generic sub-record type contained in block reference 0 of an  record. This structure is utilized when any of the following flags are set: , , or .

The structure uses a flexible array member to efficiently store a variable number of  values. In the case of the  variant, the data array contains twice the number of  as indicated by , representing redirect mappings from old to new item locations.

## Parameters / Member Variables
- : The number of target items being processed (redirected, marked as dead, or unused)
- : A flexible array of  values representing the item identifiers being processed

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [log_heap_prune_and_freeze](../l/log_heap_prune_and_freeze.md)
  - [heap_xlog_deserialize_prune_and_freeze](../h/heap_xlog_deserialize_prune_and_freeze.md)

## Notes and Other Information
- This structure is part of PostgreSQL's WAL logging mechanism for heap page pruning
- The flexible array design allows for efficient storage of variable-length data
- Used in conjunction with heap pruning and freezing operations to maintain transaction consistency
- The actual interpretation of the data array depends on the specific pruning operation flags set in the parent WAL record