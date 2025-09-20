# hashhandler

## Location
[src/backend/access/hash/hash.c:57-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash.c#L57-L114)

## Overview
Hash handler function that returns the IndexAmRoutine structure with access method parameters and callbacks for PostgreSQL hash indexes.

## Definition
```c
Datum hashhandler(PG_FUNCTION_ARGS)
```

## Detailed Description
The hashhandler function serves as the main entry point for hash index access method initialization in PostgreSQL. It creates and configures an IndexAmRoutine structure that defines all the capabilities, limitations, and callback functions for hash indexes. This function is called during index creation or when the system needs to access hash index functionality.

The function sets up all the operational parameters for hash indexes, including strategy numbers, support procedures, and various boolean flags that indicate what operations the hash access method supports or doesn't support. It also assigns all the callback functions that will be used for different hash index operations like building, inserting, scanning, and maintenance.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create IndexAmRoutine)
  - [hashbuild](hashbuild.md)
  - [hashbuildempty](hashbuildempty.md)  
  - [hashinsert](hashinsert.md)
  - [hashbulkdelete](hashbulkdelete.md)
  - [hashvacuumcleanup](hashvacuumcleanup.md)
  - [hashcostestimate](hashcostestimate.md)
  - hashoptions
  - [hashvalidate](hashvalidate.md)
  - [hashadjustmembers](hashadjustmembers.md)
  - [hashbeginscan](hashbeginscan.md)
  - [hashrescan](hashrescan.md)
  - [hashgettuple](hashgettuple.md)
  - [hashgetbitmap](hashgetbitmap.md)
  - [hashendscan](hashendscan.md)
- Called from: 
  - PostgreSQL access method system (indirectly through function registration)

## Notes and Other Information
- Sets amcanorder = false, indicating hash indexes don't support ordered scans
- Sets amcanunique = false, meaning hash indexes cannot enforce uniqueness constraints
- Sets amcanmulticol = false, restricting hash indexes to single-column keys
- Sets ampredlocks = true, enabling predicate locking for serializable isolation
- The function configures hash indexes as non-clusterable and non-parallel for building
- Hash indexes use INT4OID as their key type
- Returns the configured IndexAmRoutine via PG_RETURN_POINTER macro