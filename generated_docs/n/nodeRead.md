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

## Simplified Source

```c
void *
nodeRead(const char *token, int tok_len)
{
    Node *result;
    NodeTag type;

    // Read next token if none provided
    if (token == NULL)
    {
        token = pg_strtok(&tok_len);
        if (token == NULL)
            return NULL;  // End of input
    }

    type = nodeTokenType(token, tok_len);

    switch ((int) type)
    {
        case LEFT_BRACE:
            // Parse general node structure: { NodeType ... }
            result = parseNodeString();
            token = pg_strtok(&tok_len);
            if (token == NULL || token[0] != '}')
                elog(ERROR, "did not find '}' at end of input node");
            break;

        case LEFT_PAREN:
            {
                List *l = NIL;

                token = pg_strtok(&tok_len);
                if (token == NULL)
                    elog(ERROR, "unterminated List structure");

                if (tok_len == 1 && token[0] == 'i')
                {
                    // Integer list: (i int int ...)
                    for (;;)
                    {
                        token = pg_strtok(&tok_len);
                        if (token == NULL)
                            elog(ERROR, "unterminated List structure");
                        if (token[0] == ')')
                            break;

                        int val = (int) strtol(token, &endptr, 10);
                        if (endptr != token + tok_len)
                            elog(ERROR, "unrecognized integer");
                        l = lappend_int(l, val);
                    }
                    result = (Node *) l;
                }
                else if (tok_len == 1 && token[0] == 'o')
                {
                    // OID list: (o oid oid ...)
                    for (;;)
                    {
                        token = pg_strtok(&tok_len);
                        if (token == NULL)
                            elog(ERROR, "unterminated List structure");
                        if (token[0] == ')')
                            break;

                        Oid val = (Oid) strtoul(token, &endptr, 10);
                        if (endptr != token + tok_len)
                            elog(ERROR, "unrecognized OID");
                        l = lappend_oid(l, val);
                    }
                    result = (Node *) l;
                }
                else if (tok_len == 1 && token[0] == 'x')
                {
                    // Transaction ID list: (x xid xid ...)
                    for (;;)
                    {
                        token = pg_strtok(&tok_len);
                        if (token == NULL)
                            elog(ERROR, "unterminated List structure");
                        if (token[0] == ')')
                            break;

                        TransactionId val = (TransactionId) strtoul(token, &endptr, 10);
                        if (endptr != token + tok_len)
                            elog(ERROR, "unrecognized Xid");
                        l = lappend_xid(l, val);
                    }
                    result = (Node *) l;
                }
                else if (tok_len == 1 && token[0] == 'b')
                {
                    // Bitmapset: (b int int ...)
                    Bitmapset *bms = NULL;

                    for (;;)
                    {
                        token = pg_strtok(&tok_len);
                        if (token == NULL)
                            elog(ERROR, "unterminated Bitmapset structure");
                        if (tok_len == 1 && token[0] == ')')
                            break;

                        int val = (int) strtol(token, &endptr, 10);
                        if (endptr != token + tok_len)
                            elog(ERROR, "unrecognized integer");
                        bms = bms_add_member(bms, val);
                    }
                    result = (Node *) bms;
                }
                else
                {
                    // General node list: (node node ...)
                    for (;;)
                    {
                        if (token[0] == ')')
                            break;
                        l = lappend(l, nodeRead(token, tok_len));
                        token = pg_strtok(&tok_len);
                        if (token == NULL)
                            elog(ERROR, "unterminated List structure");
                    }
                    result = (Node *) l;
                }
                break;
            }

        case RIGHT_PAREN:
            elog(ERROR, "unexpected right parenthesis");
            result = NULL;
            break;

        case OTHER_TOKEN:
            if (tok_len == 0)
            {
                // Special null pointer representation: <>
                result = NULL;
            }
            else
            {
                elog(ERROR, "unrecognized token");
                result = NULL;
            }
            break;

        case T_Integer:
            result = (Node *) makeInteger(atoi(token));
            break;

        case T_Float:
            {
                char *fval = (char *) palloc(tok_len + 1);
                memcpy(fval, token, tok_len);
                fval[tok_len] = '\0';
                result = (Node *) makeFloat(fval);
            }
            break;

        case T_Boolean:
            result = (Node *) makeBoolean(token[0] == 't');
            break;

        case T_String:
            // Remove leading/trailing quotes and unescape
            result = (Node *) makeString(debackslash(token + 1, tok_len - 2));
            break;

        case T_BitString:
            // Remove backslashes (no quotes)
            result = (Node *) makeBitString(debackslash(token, tok_len));
            break;

        default:
            elog(ERROR, "unrecognized node type");
            result = NULL;
            break;
    }

    return (void *) result;
}
```