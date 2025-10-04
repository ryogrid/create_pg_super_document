# _readExtensibleNode

## Location
[src/backend/nodes/readfuncs.c:526-561](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/readfuncs.c#L526-L561)

## Overview
A static function that deserializes ExtensibleNode instances from their string representation using registered extension-specific deserialization methods.

## Definition

```c
static ExtensibleNode *
_readExtensibleNode(void)
```
## Detailed Description
The  function handles the deserialization of ExtensibleNode objects, which represent a framework for extending PostgreSQL's node system with custom node types. ExtensibleNodes allow extensions and plugins to define their own node types that can participate in PostgreSQL's serialization/deserialization infrastructure.

The function performs these key operations:
1. Reads the extension node name from the serialized data
2. Uses  to retrieve the method structure for the specific extension
3. Allocates a new ExtensibleNode using the size specified in the methods structure
4. Sets the extension node name in the allocated node
5. Delegates the actual deserialization of private fields to the extension's specific  method

This design allows extension authors to implement their own serialization logic while integrating seamlessly with PostgreSQL's existing node infrastructure.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - READ_TEMP_LOCALS (macro for temporary local variable setup)
  - [pg_strtok](../p/pg_strtok.md) (tokenizer function)
  - [nullable_string](../n/nullable_string.md) (helper function to process string tokens)
  - [GetExtensibleNodeMethods](../G/GetExtensibleNodeMethods.md) (retrieves method structure for extensible node type)
  - [newNode](../n/newNode.md) (allocates new node with specified size and type)
  - methods->nodeRead (extension-specific deserialization callback)
  - READ_DONE (macro for cleanup)
  - elog (error logging function)
- Called from (representative examples):
  - No direct references found (likely called via function pointer table)

## Notes and Other Information
- This is a static function, accessible only within readfuncs.c
- Part of PostgreSQL's extensible node framework that allows custom node types from extensions
- Uses the extension's own nodeRead method for deserializing private fields, maintaining encapsulation
- Validates that the extension node name is present and not null
- The method structure is retrieved using GetExtensibleNodeMethods with false flag (no missing method error)
- Allocates nodes with variable sizes as specified by the extension's method structure
- Critical for supporting custom nodes in parallel query execution and plan caching scenarios
- Extensions must register their ExtensibleNodeMethods during initialization for this to work

## Simplified Source

```c
static ExtensibleNode *
_readExtensibleNode(void)
{
    const ExtensibleNodeMethods *methods;
    ExtensibleNode *local_node;
    const char *extnodename;

    READ_TEMP_LOCALS();

    // Skip :extnodename token, get extension name
    token = pg_strtok(&length);
    token = pg_strtok(&length);

    extnodename = nullable_string(token, length);
    if (!extnodename)
        elog(ERROR, "extnodename has to be supplied");

    // Get methods for this extension type
    methods = GetExtensibleNodeMethods(extnodename, false);

    // Allocate node with extension-specified size
    local_node = (ExtensibleNode *) newNode(methods->node_size, T_ExtensibleNode);
    local_node->extnodename = extnodename;

    // Let extension deserialize its private fields
    methods->nodeRead(local_node);

    READ_DONE();
}
```