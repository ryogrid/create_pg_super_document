# nodeToString

## Location
[src/backend/nodes/outfuncs.c:791-796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L791-L796)

## Overview
A public interface function that converts a PostgreSQL node structure to its ASCII string representation without location field information.

## Definition
char *nodeToString(const void *obj)

## Detailed Description
nodeToString is one of the main externally visible entry points for converting PostgreSQL parse tree nodes and other structures into their string representations. It serves as a simple wrapper around nodeToStringInternal, specifically calling it with the write_loc_fields parameter set to false. This means that location fields in the output will be represented as -1 rather than their actual values, which is the standard behavior for most PostgreSQL operations since the original query string is typically not available or needed.

## Parameters / Member Variables
- obj: A pointer to the PostgreSQL node or structure to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - [nodeToStringInternal](nodeToStringInternal.md)
- Called from (representative examples):
  - [StoreRelCheck](../S/StoreRelCheck.md) (src/backend/catalog/heap.c:2143)
  - [UpdateIndexRelation](../U/UpdateIndexRelation.md) (src/backend/catalog/index.c:604)
  - [ProcedureCreate](../P/ProcedureCreate.md) (src/backend/catalog/pg_proc.c:332)
  - [CreatePolicy](../C/CreatePolicy.md) (src/backend/commands/policy.c:701)
  - [CreateTriggerFiringOn](../C/CreateTriggerFiringOn.md) (src/backend/commands/trigger.c:671)
  - [ExecSerializePlan](../E/ExecSerializePlan.md) (src/backend/executor/execParallel.c:216)
  - [InsertRule](../I/InsertRule.md) (src/backend/rewrite/rewriteDefine.c:60)

## Notes and Other Information
- This is the standard function used throughout PostgreSQL for node-to-string conversion
- Location fields are always output as -1, making the output consistent and suitable for storage in system catalogs
- The function is widely used across the PostgreSQL codebase for serializing parse trees, expressions, and other node structures
- Memory for the returned string is allocated using PostgreSQL's palloc mechanism
- This function is preferred over nodeToStringWithLocations for production use cases where debugging location information is not needed