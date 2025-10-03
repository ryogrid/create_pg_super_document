# TidStoreAttach

## Location
[src/backend/access/common/tidstore.c:255-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tidstore.c#L255-L279)

## Overview
Attaches to an existing shared TidStore by connecting to its Dynamic Shared Area (DSA) and creating a local handle for backend-specific access to the shared TID data.

## Definition
```c
TidStore *TidStoreAttach(dsa_handle area_handle, dsa_pointer handle)
```

## Detailed Description
TidStoreAttach allows a backend process to connect to an existing shared TidStore that was previously created by another process using TidStoreCreateShared. The function takes a DSA handle and a DSA pointer (obtained from TidStoreGetHandle()) to locate and attach to the shared TID storage. It creates a new backend-local TidStore object that provides access to the shared radix tree data.

The function performs validation to ensure both the area_handle and handle are valid before proceeding with the attachment. It creates per-backend state in local memory while connecting to the shared radix tree structure in the DSA area.

## Parameters / Member Variables
- `area_handle`: DSA handle identifying the shared memory area where the TidStore was created
- `handle`: DSA pointer (obtained from TidStoreGetHandle()) pointing to the shared radix tree structure

## Dependencies
- Functions called/Symbols referenced:
  - `Assert`
  - [palloc0](../p/palloc0.md)
  - [dsa_attach](../d/dsa_attach.md)
  - `shared_ts_attach`
  - `DSA_HANDLE_INVALID`
  - `DsaPointerIsValid`
- Called from (representative examples):
  - [parallel_vacuum_main](../p/parallel_vacuum_main.md) (src/backend/commands/vacuumparallel.c:1039)

## Notes and Other Information
- The returned TidStore object is allocated in backend-local memory using CurrentMemoryContext
- Both area_handle and handle parameters must be valid (checked with assertions)
- This function is the counterpart to TidStoreCreateShared - one process creates, others attach
- Primarily used in parallel vacuum operations where worker processes need to attach to a TidStore created by the leader process
- The area_handle and handle are typically obtained through inter-process communication mechanisms

## Simplified Source

```c
TidStore *TidStoreAttach(dsa_handle area_handle, dsa_pointer handle)
{
    Assert(area_handle != DSA_HANDLE_INVALID);
    Assert(DsaPointerIsValid(handle));

    // Create local backend state
    TidStore *ts = palloc0(sizeof(TidStore));

    // Attach to the DSA area
    dsa_area *area = dsa_attach(area_handle);

    // Connect to the shared radix tree
    ts->tree.shared = shared_ts_attach(area, handle);
    ts->area = area;

    return ts;
}
```