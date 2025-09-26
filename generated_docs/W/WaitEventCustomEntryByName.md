# WaitEventCustomEntryByName

## Location
src/backend/utils/activity/wait_event.c: 77 - 81

## Overview
A hash table entry structure used to look up custom wait event IDs by wait event name, providing the reverse mapping from human-readable names to numeric identifiers.

## Definition


## Detailed Description
WaitEventCustomEntryByName is a hash table entry structure that complements WaitEventCustomEntryByInfo by providing the reverse mapping in PostgreSQL's custom wait event system. While WaitEventCustomEntryByInfo maps from numeric IDs to names, this structure maps from wait event names to their corresponding numeric identifiers. This bidirectional mapping system enables efficient lookups in both directions, supporting various wait event management operations such as registration, identification, and enumeration.

The structure is used in shared memory hash tables to ensure that custom wait events can be quickly located and referenced by name across different PostgreSQL processes.

## Parameters / Member Variables
- : A character array of size NAMEDATALEN serving as the hash key, containing the human-readable name of the custom wait event
- : A 32-bit unsigned integer containing the unique numeric identifier for the custom wait event

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN (constant defining maximum length for names)
- Called from (representative examples):
  - WaitEventCustomShmemSize
  - WaitEventCustomShmemInit
  - WaitEventCustomNew
  - GetWaitEventCustomNames

## Notes and Other Information
- This structure forms part of a dual hash table system alongside WaitEventCustomEntryByInfo for bidirectional lookups
- The name-based hash key allows for string-based lookups when registering new custom wait events or checking for duplicates
- Used extensively during custom wait event creation to prevent duplicate registrations
- Stored in shared memory for cross-process accessibility in PostgreSQL's multi-process architecture
- Essential for the GetWaitEventCustomNames function which retrieves all custom wait event names