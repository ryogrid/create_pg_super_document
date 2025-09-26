# ApplySubXactData

## Location
src/backend/replication/logical/worker.c: 349 - 355

## Overview
ApplySubXactData is a structure that manages metadata for subtransactions within the current streaming transaction during logical replication processing.

## Definition


## Detailed Description
ApplySubXactData serves as a container for managing subtransaction metadata during streaming logical replication. When a large transaction is being streamed and contains multiple subtransactions, this structure tracks the collection of subtransactions, maintaining both current count and capacity for dynamic array management. It provides efficient access to subtransaction information stored in temporary files through the SubXactInfo array, enabling the logical replication worker to properly handle complex transaction hierarchies during the apply process.

## Parameters / Member Variables
- : Current number of subtransactions being tracked
- : Maximum capacity of the subxacts array (for dynamic resizing)
- : TransactionId of the most recently processed subtransaction
- : Pointer to an array of SubXactInfo structures containing file location metadata for each subtransaction

## Dependencies
- Functions called/Symbols referenced:
  - SubXactInfo
  - TransactionId
  - uint32
- Called from (representative examples):
  - Currently appears to be part of internal streaming transaction management
  - Used in conjunction with SubXactInfo for subtransaction file management

## Notes and Other Information
This structure implements a dynamic array pattern for managing subtransaction metadata, with nsubxacts_max tracking capacity to allow for efficient resizing. The subxact_last field helps optimize processing by tracking the most recent subtransaction. The structure works closely with SubXactInfo to provide a complete solution for managing subtransaction data that has been spilled to temporary files during large streaming transaction processing in logical replication.