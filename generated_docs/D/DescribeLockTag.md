# DescribeLockTag

## Location
src/backend/storage/lmgr/lmgr.c: 1239 - 1335

## Overview
Generates a human-readable description of a lockable object from its lock tag, primarily used for error reporting and debugging deadlock situations.

## Definition
```c
void DescribeLockTag(StringInfo buf, const LOCKTAG *tag)
```

## Detailed Description
This function appends a human-readable description of a lockable object to a StringInfo buffer based on the provided LOCKTAG. It uses a switch statement to handle different lock tag types and formats appropriate descriptions for each type. The function is designed to avoid acquiring additional locks on system tables (which could cause problems during deadlock reporting) by using only the numeric identifiers stored in the lock tag fields. The descriptions are internationalized using the _() macro for translation support.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the description will be appended
- `tag`: Pointer to the LOCKTAG structure containing the lock information to describe

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfo (formats and appends text to the buffer)
  - LockTagType enumeration values (LOCKTAG_RELATION, LOCKTAG_TUPLE, etc.)
  - Internationalization macro _() for translatable strings
- Called from (representative examples):
  - DeadLockReport (for deadlock error reporting)
  - ProcSleep (when logging lock wait information)

## Notes and Other Information
- Handles all major lock tag types including relations, pages, tuples, transactions, virtual transactions, speculative tokens, database objects, user locks, advisory locks, and apply transactions
- Uses locktag_field1 through locktag_field4 to extract relevant identifiers based on lock type
- Designed to be safe during deadlock detection when acquiring additional locks could be problematic
- Each lock type has a specific format showing the most relevant identifiers (database ID, relation ID, page number, tuple coordinates, etc.)
- The LOCKTAG_USERLOCK case includes a comment noting it's reserved for old contrib code
- Includes a default case for unrecognized lock tag types to ensure robust error handling