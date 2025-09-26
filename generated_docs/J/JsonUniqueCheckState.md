# JsonUniqueCheckState

## Location
[src/backend/utils/adt/json.c:39-41](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L39-L41)

## Overview
JsonUniqueCheckState is a typedef for a hash table pointer (HTAB *) used to maintain fast key uniqueness checking in JSON objects by tracking used key names to detect duplicates efficiently.

## Definition

```c
typedef struct HTAB *JsonUniqueCheckState;
```
## Detailed Description
JsonUniqueCheckState serves as the core data structure for PostgreSQL's fast JSON key uniqueness validation system. It's essentially a hash table that stores JSON object key names to quickly detect duplicate keys during JSON parsing and processing operations. This mechanism ensures JSON objects comply with the JSON specification requirement that object keys be unique, while providing O(1) average-case lookup performance for duplicate detection.

The hash table is dynamically allocated and managed through PostgreSQL's hash table infrastructure (HTAB), providing efficient memory management and collision resolution for key name storage.

## Parameters / Member Variables
As this is a typedef for HTAB *, the actual structure members are those of PostgreSQL's standard hash table:
- Hash table contains JsonUniqueHashEntry structures as entries
- Manages memory allocation and hash bucket organization internally
- Provides standard hash table operations (insert, lookup, delete)

## Dependencies
- Functions called/Symbols referenced:
  - HTAB (PostgreSQL hash table infrastructure)
- Called from (representative examples):
  - json_unique_check_init
  - json_unique_check_key
  - JsonUniqueParsingState (as member)
  - JsonUniqueBuilderState (as member)

## Notes and Other Information
- This is part of PostgreSQL's JSON processing optimization infrastructure
- Used in conjunction with JsonUniqueHashEntry for actual key storage
- Integrated into both parsing and building contexts for comprehensive uniqueness checking
- Provides significant performance improvement over linear search methods for duplicate key detection
- The hash table is typically initialized with appropriate sizing based on expected JSON object complexity