# ComboCidEntryData

## Location
src/backend/utils/time/combocid.c: 68 - 69

## Overview
ComboCidEntryData is a structure that represents an entry in the hash table used to store the mapping between (cmin, cmax) command ID pairs and their corresponding combo command IDs in PostgreSQL's combo command ID system.

## Definition

```c
typedef ComboCidEntryData *ComboCidEntry;
```
## Detailed Description
ComboCidEntryData is the hash table entry structure used in PostgreSQL's combo command ID optimization system. Each entry in the comboHash hash table contains both the key (a ComboCidKeyData structure with cmin and cmax values) and the associated combo command ID value.

This structure is central to PostgreSQL's tuple header size optimization introduced in version 8.3. When a transaction both inserts and deletes the same tuple, PostgreSQL needs to store both command IDs but wants to save space in the tuple header. The combo command ID system creates a single ID that maps to the original (cmin, cmax) pair through this hash table structure.

The hash table allows for efficient lookup and reuse of existing combo CIDs when the same (cmin, cmax) pair is encountered multiple times within a transaction, keeping the overall data structure size reasonable.

## Parameters / Member Variables
- : A ComboCidKeyData structure containing the cmin and cmax command IDs that serve as the hash table key
- : The combo command ID value that corresponds to the (cmin, cmax) pair in the key

## Dependencies
- Functions called/Symbols referenced:
  - ComboCidKeyData (embedded structure)
  - CommandId (type)
- Called from (representative examples):
  - ComboCidEntry (typedef pointer to this structure)
  - GetComboCommandId (creates and manipulates entries of this type)

## Notes and Other Information
- This structure is used exclusively within the combocid.c module as part of the combo command ID hash table implementation
- The structure combines both the search key and the mapped value, following standard hash table entry patterns
- Entries are stored in the comboHash hash table, which is kept in TopTransactionContext and destroyed at the end of each transaction
- The hash table using these entries has an initial size of CCID_HASH_SIZE (100) entries
- This optimization allows PostgreSQL to overlay cmin and cmax fields in HeapTupleHeaderData while still maintaining the ability to retrieve the original values when needed
- The combo command ID system can handle up to 2^32 distinct cmin,cmax combinations with 32-bit combo command IDs