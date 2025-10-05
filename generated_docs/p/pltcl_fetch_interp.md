# pltcl_fetch_interp

## Location
[src/pl/tcl/pltcl.c:563-592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L563-L592)

## Overview
Retrieves or creates a Tcl interpreter for executing PL/Tcl functions, managing per-user interpreter instances and handling lazy initialization.

## Definition

```c
static pltcl_interp_desc *
pltcl_fetch_interp(Oid prolang, bool pltrusted)
```
## Detailed Description
The `pltcl_fetch_interp` function is responsible for providing the appropriate Tcl interpreter for executing PL/Tcl functions. It implements a per-user interpreter caching system where:

1. For trusted PL/Tcl, each database user gets their own interpreter (identified by user ID)
2. For untrusted PL/Tcl, a single shared interpreter is used (identified by InvalidOid)

The function uses a hash table (`pltcl_interp_htab`) to store interpreter descriptors and performs lazy initialization - interpreters are only created when first needed. If an interpreter descriptor exists but the interpreter hasn't been initialized yet, it calls `pltcl_init_interp` to create and set up the interpreter.

This design provides security isolation between users in trusted mode while allowing efficient resource sharing in untrusted mode.

## Parameters / Member Variables
- `prolang`: OID of the procedural language (pltcl or pltclu) 
- `pltrusted`: Boolean indicating whether this is for trusted (true) or untrusted (false) PL/Tcl

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md) (retrieves current user ID for trusted mode)
  - [hash_search](../h/hash_search.md) (searches/creates hash table entries)
  - [pltcl_init_interp](pltcl_init_interp.md) (initializes new interpreters)
  - `InvalidOid` (constant for untrusted mode key)
  - `HASH_ENTER` (hash table operation flag)
- Called from (representative examples):
  - `[compile_pltcl_function](../c/compile_pltcl_function.md)` (when compiling PL/Tcl functions)

## Notes and Other Information
- Uses lazy initialization pattern - interpreters are created only when first accessed
- Implements per-user security isolation for trusted PL/Tcl through separate interpreters
- Untrusted PL/Tcl uses a single shared interpreter for all users
- The function is static, indicating it's only used within the pltcl.c module
- [Hash](../H/Hash.md) table key is user_id for trusted mode, InvalidOid for untrusted mode
- Returns a pointer to the interpreter descriptor, never NULL (creates if needed)

## Simplified Source

```c
static pltcl_interp_desc *pltcl_fetch_interp(Oid prolang, bool pltrusted) {
    Oid user_id;
    pltcl_interp_desc *interp_desc;
    bool found;

    // Determine user ID for hash table key
    if (pltrusted)
        user_id = GetUserId();    // Per-user interpreter for trusted
    else
        user_id = InvalidOid;     // Shared interpreter for untrusted

    // Find or create interpreter descriptor in hash table
    interp_desc = hash_search(pltcl_interp_htab, &user_id,
                             HASH_ENTER, &found);
    if (!found)
        interp_desc->interp = NULL;

    // Initialize interpreter if not already done
    if (!interp_desc->interp)
        pltcl_init_interp(interp_desc, prolang, pltrusted);

    return interp_desc;
}
```