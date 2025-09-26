# WaitEventCustomEntryByInfo

## Location
[src/backend/utils/activity/wait_event.c:71-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/wait_event.c#L71-L75)

## Overview
A hash table entry structure used to look up custom wait event information by wait event ID, enabling efficient mapping from wait event identifiers to their associated names.

## Definition

```c
typedef struct WaitEventCustomEntryByInfo
{
	uint32		wait_event_info;	/* hash key */
	char		wait_event_name[NAMEDATALEN];	/* custom wait event name */
} WaitEventCustomEntryByInfo;
```
## Detailed Description
WaitEventCustomEntryByInfo is a hash table entry structure that serves as part of PostgreSQL's custom wait event management system. It provides a mapping from wait event information (numeric identifier) to the corresponding human-readable wait event name. This structure is used in hash tables to enable fast lookups when translating wait event IDs to their string representations, which is essential for wait event reporting and monitoring functionality.

The structure is designed for use in shared memory hash tables where multiple processes need to access wait event information concurrently.

## Parameters / Member Variables
- `wait_event_info`: A 32-bit unsigned integer serving as the hash key, containing the unique identifier for the custom wait event
- `wait_event_name[NAMEDATALEN]`: A character array of size NAMEDATALEN containing the human-readable name of the custom wait event
## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN (constant defining maximum length for names)
- Called from (representative examples):
  - [WaitEventCustomShmemSize](WaitEventCustomShmemSize.md)
  - [WaitEventCustomShmemInit](WaitEventCustomShmemInit.md)
  - [WaitEventCustomNew](WaitEventCustomNew.md)
  - [GetWaitEventCustomIdentifier](../G/GetWaitEventCustomIdentifier.md)

## Notes and Other Information
- This structure is part of a dual hash table system where WaitEventCustomEntryByName provides the reverse mapping (name to ID)
- The structure is stored in shared memory to be accessible across different PostgreSQL processes
- NAMEDATALEN is typically 64 bytes, providing sufficient space for descriptive wait event names
- Used primarily for translating wait event IDs back to readable names for monitoring and diagnostic purposes