# ResetTempTableNamespace

## Location
[src/backend/catalog/namespace.c:4644-4656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4644-L4656)

## Overview
ResetTempTableNamespace provides a simple interface to remove all temporary tables from the current session's temporary namespace.

## Definition


## Detailed Description
This function serves as a clean, public interface for removing all temporary relations from the current backend's temporary namespace. It acts as a lightweight wrapper around RemoveTempRelations, providing a simple way to clear temporary objects without requiring knowledge of the internal namespace management details.

The function:
1. Checks if a valid temporary namespace exists for the current session
2. If one exists, delegates to RemoveTempRelations to perform the actual cleanup
3. Provides no return value - operates silently

This function is typically used in scenarios where a complete reset of temporary objects is desired, such as when processing DISCARD TEMP or DISCARD ALL commands.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [RemoveTempRelations](RemoveTempRelations.md)
- Called from (representative examples):
  - [DiscardCommand](../D/DiscardCommand.md)
  - [DiscardAll](../D/DiscardAll.md)

## Notes and Other Information
- This is a public function (not static) that can be called from other PostgreSQL modules
- The function is safe to call even if no temporary namespace exists - it will simply do nothing
- Uses the global variable myTempNamespace to identify the current session's temporary namespace
- Part of PostgreSQL's DISCARD command infrastructure, allowing users to explicitly clean up temporary objects
- Much simpler than the callback version since it doesn't need to handle transaction management - assumes it's called within an existing transaction context