# json_unique_check_init

## Location
[src/backend/utils/adt/json.c:923-940](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L923-L940)

## Overview
The  function initializes a hash table for detecting duplicate keys during JSON object construction and parsing operations.

## Definition

```c
static void
json_unique_check_init(JsonUniqueCheckState *cxt)
```
## Detailed Description
This function creates and configures a hash table specifically designed for JSON key uniqueness checking. It sets up a PostgreSQL HTAB with appropriate hash and comparison functions, using  structures as both keys and entries. The hash table is initialized with a default capacity of 32 entries and uses the current memory context for allocation.

The function configures the hash table with custom hash and match functions ( and ) that properly handle the composite key structure containing object IDs, key strings, and key lengths. This enables efficient detection of duplicate keys within JSON objects during aggregation or validation operations.

## Parameters / Member Variables
- : Pointer to a  variable that will receive the initialized hash table handle

## Dependencies
- Functions called/Symbols referenced:
  - : Type alias for HTAB pointer used for key uniqueness checking
  - : PostgreSQL hash table control structure for configuration
  - : Structure defining the hash table entry format
  - : Global variable for the current memory allocation context
  - : Custom hash function for computing hash values
  - : Custom comparison function for key matching
  - : PostgreSQL function to create a new hash table
  - : Flag indicating custom element size
  - : Flag indicating custom memory context
  - : Flag indicating custom hash function
  - : Flag indicating custom comparison function

- Called from (representative examples):
  - : Initializes uniqueness checking for JSON builders
  - : Sets up uniqueness checking during JSON validation

## Notes and Other Information
- This is a static function internal to the JSON aggregate implementation
- Creates a hash table with initial capacity of 32 entries, which can grow dynamically
- Uses the current memory context for hash table allocation, ensuring proper cleanup
- The hash table configuration enables both custom hashing and comparison logic
- Essential for preventing duplicate keys in JSON objects, which would violate JSON standards
- Part of PostgreSQL's comprehensive JSON validation and construction infrastructure
- The hash table name "json object hashtable" is used for debugging and monitoring purposes

## Simplified Source

```c
static void
json_unique_check_init(JsonUniqueCheckState *cxt)
{
    HASHCTL ctl;

    // Set up hash table configuration
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(JsonUniqueHashEntry);
    ctl.entrysize = sizeof(JsonUniqueHashEntry);
    ctl.hcxt = CurrentMemoryContext;
    ctl.hash = json_unique_hash;
    ctl.match = json_unique_hash_match;

    // Create hash table for JSON key uniqueness checking
    *cxt = hash_create("json object hashtable",
                       32,
                       &ctl,
                       HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE);
}
```