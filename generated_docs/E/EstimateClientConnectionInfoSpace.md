# EstimateClientConnectionInfoSpace

## Location
[src/backend/utils/init/miscinit.c:1081-1096](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L1081-L1096)

## Overview
Calculates the space needed to serialize MyClientConnectionInfo for parallel worker processes or other contexts requiring serialization.

## Definition
```c
Size EstimateClientConnectionInfoSpace(void)
```

## Detailed Description
This function computes the total memory space required to serialize the current backend's client connection information (stored in the global `MyClientConnectionInfo`). The calculation includes the fixed-size structure `SerializedClientConnectionInfo` plus variable-length data such as the authentication identifier string. This is typically used in parallel query execution where client connection information needs to be passed to worker processes.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [add_size](../a/add_size.md) - Safe integer addition function that handles overflow
  - [SerializedClientConnectionInfo](../S/SerializedClientConnectionInfo.md) - Structure type for serialized connection info
  - `MyClientConnectionInfo` - Global variable containing current client connection info
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) - When setting up parallel query execution
  - `INIT_PG_OVERRIDE_ROLE_LOGIN` - In role login override scenarios

## Notes and Other Information
- Returns a `Size` type value representing the total bytes needed
- The calculation is safe from integer overflow due to use of `add_size()`
- Only includes space for `authn_id` if it exists (null pointer check)
- Part of the client connection info serialization API along with `SerializeClientConnectionInfo` and `RestoreClientConnectionInfo`

## Simplified Source

```c
Size EstimateClientConnectionInfoSpace(void) {
    Size size = 0;

    // Account for fixed-size structure
    size = add_size(size, sizeof(SerializedClientConnectionInfo));

    // Add space for authentication ID string if present
    if (MyClientConnectionInfo.authn_id)
        size = add_size(size, strlen(MyClientConnectionInfo.authn_id) + 1);

    return size;
}
```