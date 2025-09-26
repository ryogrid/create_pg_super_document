# ComboCidKeyData

## Location
src/backend/utils/time/combocid.c: 60 - 61

## Overview
ComboCidKeyData is a structure that serves as a key in a hash table used to map (cmin, cmax) command ID pairs to combo command IDs in PostgreSQL's combo command ID system.

## Definition


## Detailed Description
ComboCidKeyData is a fundamental component of PostgreSQL's combo command ID optimization system introduced in version 8.3. This structure represents a key in a hash table (comboHash) that maps pairs of command IDs (cmin and cmax) to single combo command IDs. 

The combo command ID system was designed to reduce the size of tuple headers by overlaying the cmin and cmax fields. When a transaction both inserts and deletes the same tuple, PostgreSQL creates a "combo" command ID that can be mapped back to the original cmin and cmax values using a backend-private hash table. The ComboCidKeyData structure serves as the search key for this hash table, allowing efficient lookup of existing combo CIDs for reuse.

This optimization is particularly important because it keeps the data structure size reasonable in most cases, since the number of unique (cmin, cmax) pairs used by any single transaction is typically small.

## Parameters / Member Variables
- : The command ID of the command that inserted the tuple
- : The command ID of the command that deleted/updated the tuple

## Dependencies
- Functions called/Symbols referenced:
  - CommandId (type)
- Called from (representative examples):
  - ComboCidKey (typedef pointer to this structure)
  - ComboCidEntryData (contains this as a member)
  - GetComboCommandId (uses this structure for hash table operations)
  - EstimateComboCIDStateSpace (references this structure for size calculations)
  - SerializeComboCIDState (uses this structure during serialization)
  - RestoreComboCIDState (uses this structure during deserialization)

## Notes and Other Information
- This structure is used exclusively within the combocid.c module as part of the combo command ID hash table implementation
- The structure is designed to be lightweight with only two CommandId fields
- It's part of PostgreSQL's tuple header size optimization that overlays cmin and cmax fields in HeapTupleHeaderData
- The hash table using this key is kept in TopTransactionContext and destroyed at the end of each transaction
- With 32-bit combo command IDs, PostgreSQL can represent 2^32 distinct cmin,cmax combinations