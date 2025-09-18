# pg_node_tree_send

## Location
src/backend/utils/adt/pseudotypes.c: 344 - 377

## Overview
A binary output function for the pg_node_tree pseudo-type that serializes pg_node_tree values to binary format by delegating to the textsend function.

## Definition
Datum pg_node_tree_send(PG_FUNCTION_ARGS)

## Detailed Description
The pg_node_tree_send function is a binary output (send) function for the pg_node_tree pseudo-type in PostgreSQL. Unlike many other pseudo-types that have dummy I/O functions that reject all operations, pg_node_tree allows output operations to support displaying query parse trees and other internal node structures.

This function implements the binary serialization protocol for pg_node_tree values by simply calling the textsend function, treating the pg_node_tree data as text. This is appropriate because pg_node_tree values are stored internally as text representations of parse trees or other node structures.

The pg_node_tree type is used internally by PostgreSQL to store serialized representations of parse trees, plan trees, and other node-based data structures. While input operations are blocked for security reasons (to prevent malformed parse trees from being injected), output operations like this send function are allowed to enable inspection and debugging of these internal structures.

## Parameters / Member Variables
- : Function call information structure containing the pg_node_tree value to be sent and other context needed for the binary output operation

## Dependencies
- Functions called/Symbols referenced:
  - textsend (delegates binary serialization to this text send function)
- Called from (representative examples):
  - PostgreSQL's type system when binary output is requested for pg_node_tree values
  - Client protocols that require binary format data transmission

## Notes and Other Information
- Part of the pg_node_tree type's I/O function suite in src/backend/utils/adt/pseudotypes.c:344-377
- Unlike input functions (pg_node_tree_in, pg_node_tree_recv) which are blocked for security, output functions are permitted
- The function leverages the existing text binary serialization infrastructure by delegating to textsend
- This is consistent with pg_node_tree's internal storage as text representations of node structures
- Binary send/receive functions are used for efficient client-server communication in PostgreSQL's wire protocol