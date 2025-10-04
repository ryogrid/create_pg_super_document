# RegisterCustomScanMethods

## Location
[src/backend/nodes/extensible.c:88-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/extensible.c#L88-L99)

## Overview
Registers a new type of custom scan node in PostgreSQL's extensible node system, enabling extensions to provide custom scan implementations.

## Definition
```c
void RegisterCustomScanMethods(const CustomScanMethods *methods)
```

## Detailed Description
This function serves as the public API for registering custom scan node types in PostgreSQL. It acts as a wrapper around the internal RegisterExtensibleNodeEntry function, specifically designed for custom scan methods (as opposed to general extensible nodes). The function registers the provided custom scan method structure in the global custom_scan_methods hash table, using the CustomName field from the methods structure as the key. This registration enables the custom scan provider to be used in query execution plans when the optimizer determines it would be beneficial.

## Parameters / Member Variables
- `methods`: Pointer to CustomScanMethods structure containing the scan provider name and callback functions for this custom scan type

## Dependencies
- Functions called/Symbols referenced:
  - [RegisterExtensibleNodeEntry](RegisterExtensibleNodeEntry.md)
- Data types used:
  - [CustomScanMethods](../C/CustomScanMethods.md)
- Called from (representative examples):
  - (No direct references found - likely called from extension modules providing custom scan methods)

## Notes and Other Information
- This is the primary entry point for extensions to register custom scan providers
- Uses the global custom_scan_methods hash table for storage
- The methods parameter must contain a valid CustomName field
- Extensions typically call this function during module initialization
- Custom scan providers are used by the PostgreSQL optimizer when planning queries
- Registration is permanent for the duration of the PostgreSQL session
- Examples of custom scan providers include foreign data wrappers and parallel processing extensions

## Simplified Source

```c
void RegisterCustomScanMethods(const CustomScanMethods *methods) {
    // Register the custom scan methods in the global hash table
    RegisterExtensibleNodeEntry(&custom_scan_methods,
                               "Custom Scan Methods",
                               methods->CustomName,
                               methods);
}
```