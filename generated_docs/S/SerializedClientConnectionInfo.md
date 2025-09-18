# SerializedClientConnectionInfo

## Location
[src/backend/utils/init/miscinit.c:1071-1075](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L1071-L1075)

## Overview
A struct used as an intermediate representation of ClientConnectionInfo for easier serialization, particularly for passing client connection information to parallel worker processes.

## Definition


## Detailed Description
SerializedClientConnectionInfo provides a compact, serializable format for storing client connection authentication information. It serves as a binary-safe representation that can be easily transmitted between processes, especially when spawning parallel workers that need access to the original client's authentication context. The struct uses a fixed-size header followed by variable-length data, where string fields are stored immediately after the struct in memory.

The design follows PostgreSQL's pattern for serializable structures: fixed-size fields are stored directly in the struct, while variable-length fields (like authentication identifiers) are stored as separate data following the struct, with their lengths encoded in the fixed portion.

## Parameters / Member Variables
- : Length of the authentication identifier string, or -1 if the authn_id is NULL. This allows the deserializer to determine whether an authentication identifier is present and how much space to allocate for it.
- : The authentication method used by the client, stored as a UserAuth enum value representing the specific authentication mechanism (e.g., trust, md5, scram-sha-256, etc.).

## Dependencies
- Functions called/Symbols referenced:
  - UserAuth (enum type from libpq/hba.h)
- Called from (representative examples):
  - [EstimateClientConnectionInfoSpace](../E/EstimateClientConnectionInfoSpace.md)
  - [SerializeClientConnectionInfo](SerializeClientConnectionInfo.md)
  - [RestoreClientConnectionInfo](../R/RestoreClientConnectionInfo.md)

## Notes and Other Information
- [Variable](../V/Variable.md)-length fields (specifically authn_id strings) are allocated immediately after this header in memory, following PostgreSQL's serialization conventions
- The struct is used in conjunction with MyClientConnectionInfo global variable for serialization/deserialization operations
- Part of the parallel worker infrastructure that ensures worker processes have access to the same authentication context as the main backend process
- Located in src/backend/utils/init/miscinit.c:1071-1075