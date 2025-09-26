# nodeRead

## Location
[src/backend/nodes/read.c:320-511](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/read.c#L320-L511)

## Overview
A higher-level reader function that parses various types of node structures from tokenized input, including value nodes, general nodes, and different types of lists.

## Definition

```c
structure");
```
## Detailed Description
The  function provides semantic parsing capabilities on top of the lexical tokenizer . It can deserialize a wide variety of PostgreSQL internal structures including individual value nodes (integers, floats, booleans, strings), general nodes (via ), and specialized list structures.

The function handles several types of list structures with specific prefixes: integer lists (i), OID lists (o), TransactionId lists (x), bitmapsets (b), and general node lists. It uses recursive parsing for nested structures and applies appropriate type conversions and validation.

The function is designed to work within PostgreSQL's  operation framework, assuming that  has already been initialized with input data. It supports both external calls (with NULL token) and internal recursive calls (with pre-scanned tokens).

## Parameters / Member Variables
- : Pre-scanned token string, or NULL if a new token needs to be read
- : Length of the token string (ignored if token is NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strtok](../p/pg_strtok.md)
  - [nodeTokenType](nodeTokenType.md)
  - [parseNodeString](../p/parseNodeString.md)
  - [lappend_int](../l/lappend_int.md), lappend_oid, lappend_xid
  - [bms_add_member](../b/bms_add_member.md)
  - [makeInteger](../m/makeInteger.md), makeFloat, makeBoolean, makeString, makeBitString
  - [debackslash](../d/debackslash.md)
  - LEFT_BRACE, LEFT_PAREN, RIGHT_PAREN, OTHER_TOKEN
- Called from (representative examples):
  - [stringToNodeInternal](../s/stringToNodeInternal.md)
  - [nodeRead](nodeRead.md) (recursive calls)
  - READ_NODE_FIELD
  - [_readA_Const](../r/_readA_Const.md)
  - [_readA_Expr](../r/_readA_Expr.md)
  - [_readExtensibleNode](../r/_readExtensibleNode.md)

## Notes and Other Information
- Returns  instead of  to avoid casting in callers that assign to different field types
- External callers should always pass NULL/0 for arguments; non-NULL tokens are used internally for recursion
- Handles special list formats:  for integers,  for OIDs,  for XIDs,  for bitmapsets
- Supports general node lists  with recursive parsing
- Processes node structures enclosed in braces  via 
- Handles the special null pointer representation 
- Provides comprehensive error reporting for malformed input structures
- Critical component of PostgreSQL's node serialization/deserialization system