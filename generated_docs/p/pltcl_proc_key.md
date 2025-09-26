# pltcl_proc_key

## Location
[src/pl/tcl/pltcl.c:190-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L190-L200)

## Overview
A hash key structure used for fast lookup of cached PL/Tcl procedure descriptors, combining function identity with user context and trigger status.

## Definition
```c
typedef struct pltcl_proc_key
{
    Oid         proc_id;        /* Function OID */
    
    /*
     * is_trigger is really a bool, but declare as Oid to ensure this struct
     * contains no padding
     */
    Oid         is_trigger;     /* is it a trigger function? */
    Oid         user_id;        /* User calling the function, or 0 */
} pltcl_proc_key;
```

## Detailed Description
The `pltcl_proc_key` structure serves as a composite hash key for PostgreSQL's internal hash table that maps function identifiers to cached `pltcl_proc_desc` pointers. This multi-component key enables efficient lookup while handling the security and execution context requirements of PL/Tcl.

The key design addresses several critical aspects:
- **Function Identity**: Uses the PostgreSQL function OID (`proc_id`) as the primary identifier
- **Execution Context**: Distinguishes between regular functions and trigger functions via `is_trigger`
- **Security Separation**: Separates cached procedures by user ID for trusted (pltcl) functions, ensuring privilege isolation

For trusted functions (pltcl), each user gets separate cache entries to prevent privilege escalation attacks. For untrusted functions (pltclu), the `user_id` is set to 0, allowing shared cache entries since security isolation is not required.

The structure is carefully designed to avoid padding bytes, using Oid type for the boolean `is_trigger` field to maintain consistent memory layout and hash performance.

## Parameters / Member Variables
- `proc_id`: PostgreSQL object identifier (OID) of the function being cached, serving as the primary key component
- `is_trigger`: Boolean flag (stored as Oid to avoid padding) indicating whether this is a trigger function versus a regular function
- `user_id`: OID of the user calling the function; set to 0 for untrusted (pltclu) functions to enable sharing, or the actual user OID for trusted (pltcl) functions to ensure security isolation

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - pltcl_proc_ptr (referenced at line 204)
  - _PG_init (referenced at line 455)
  - compile_pltcl_function (referenced at line 1405)

## Notes and Other Information
- The structure is designed to contain no padding bytes for optimal hash table performance
- The `is_trigger` field uses Oid type instead of bool specifically to avoid memory padding
- For trusted functions (pltcl), multiple cache entries can exist for the same function when called by different users
- For untrusted functions (pltclu), only one cache entry exists regardless of the calling user (user_id = 0)
- This key structure enables the separation between the hash key and the actual procedure descriptor, simplifying error recovery during function compilation
- The hash table mapping is maintained for speedy lookup during function calls
- Located in src/pl/tcl/pltcl.c at lines 190-200