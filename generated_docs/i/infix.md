# infix

## Location
[src/backend/utils/adt/tsquery.c:991-1145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L991-L1145)

## Overview
The  function recursively traverses a TSQuery tree structure and converts it into human-readable infix notation string representation, handling operator precedence and parentheses.

## Definition

```c
static void
infix(INFIX *in, int parentPriority, bool rightPhraseOp)
```
## Detailed Description
This function is a core component of PostgreSQL's TSQuery output formatting system. It performs a recursive tree traversal of the internal TSQuery representation (stored in prefix/polish notation) and converts it back to the familiar infix notation that users expect to see. The function handles three main types of query items:

1. **Query Values (QI_VAL)**: Formats operand terms with proper quoting, escaping, weight annotations (:A, :B, :C, :D), and prefix indicators (:*)
2. **NOT operators (OP_NOT)**: Handles unary negation with appropriate precedence and parentheses
3. **Binary operators**: Manages AND (&), OR (|), and PHRASE (<->, <N>) operators with proper precedence rules

The function implements sophisticated precedence handling to minimize unnecessary parentheses while maintaining semantic correctness. It also includes special logic for phrase operators since they are order-dependent and require careful parenthesization.

The output buffer is dynamically resized as needed to accommodate the growing string representation, and proper character encoding is handled for multi-byte characters.

## Parameters / Member Variables
- `*in`: INFIX structure containing the output buffer, current position, operand strings, and current query item pointer
- `parentPriority`: Priority level of the parent operator, used for parentheses decision-making
- `rightPhraseOp`: Boolean flag indicating whether this is the right operand of a phrase operator (affects precedence rules)
## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - RESIZEBUF
  - t_iseq
  - COPYCHAR
  - [pg_mblen](../p/pg_mblen.md)
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md)
  - QO_PRIORITY
  - sprintf
  - strchr
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [infix](infix.md) (recursive calls)
  - [tsqueryout](../t/tsqueryout.md)
  - [tsquerytree](../t/tsquerytree.md)

## Notes and Other Information
- This is a static function, only accessible within the tsquery.c module
- Includes stack depth checking to prevent stack overflow on deeply nested queries
- Properly handles character escaping for single quotes and backslashes in operand values
- Implements correct operator precedence: NOT (highest) > PHRASE > AND > OR (lowest)
- Special handling for phrase operators since they are not associative and order matters
- Uses dynamic buffer allocation with RESIZEBUF macro for efficient memory management
- Supports multi-byte character encodings through pg_mblen and related functions
- The function modifies the INFIX structure's current pointer as it builds the output string

## Simplified Source

```c
static void
infix(INFIX *in, int parentPriority, bool rightPhraseOp)
{
    // Prevent stack overflow during recursion
    check_stack_depth();

    if (in->curpol->type == QI_VAL)
    {
        // Format value operand with quotes and weights
        QueryOperand *curpol = &in->curpol->qoperand;
        char *op = in->op + curpol->distance;
        int clen;

        // Allocate space for quoted value and weights
        RESIZEBUF(in, curpol->length * (pg_database_encoding_max_length() + 1) + 2 + 6);

        // Add opening quote
        *(in->cur) = '\'';
        in->cur++;

        // Copy operand string with proper escaping
        while (*op)
        {
            if (t_iseq(op, '\'') || t_iseq(op, '\\'))
            {
                // Escape quotes and backslashes
                *(in->cur) = *op;
                in->cur++;
            }
            COPYCHAR(in->cur, op);
            clen = pg_mblen(op);
            op += clen;
            in->cur += clen;
        }

        // Add closing quote
        *(in->cur) = '\'';
        in->cur++;

        // Add weight and prefix annotations if present
        if (curpol->weight || curpol->prefix)
        {
            *(in->cur) = ':';
            in->cur++;

            if (curpol->prefix)
            {
                *(in->cur) = '*';
                in->cur++;
            }

            // Add weight letters A, B, C, D based on bit flags
            if (curpol->weight & (1 << 3)) { *(in->cur) = 'A'; in->cur++; }
            if (curpol->weight & (1 << 2)) { *(in->cur) = 'B'; in->cur++; }
            if (curpol->weight & (1 << 1)) { *(in->cur) = 'C'; in->cur++; }
            if (curpol->weight & 1) { *(in->cur) = 'D'; in->cur++; }
        }

        *(in->cur) = '\0';
        in->curpol++;
    }
    else if (in->curpol->qoperator.oper == OP_NOT)
    {
        // Handle unary NOT operator
        int priority = QO_PRIORITY(in->curpol);

        // Add parentheses if needed for precedence
        if (priority < parentPriority)
        {
            RESIZEBUF(in, 2);
            sprintf(in->cur, "( ");
            in->cur = strchr(in->cur, '\0');
        }

        // Add NOT operator
        RESIZEBUF(in, 1);
        *(in->cur) = '!';
        in->cur++;
        *(in->cur) = '\0';
        in->curpol++;

        // Recursively format operand
        infix(in, priority, false);

        // Close parentheses if needed
        if (priority < parentPriority)
        {
            RESIZEBUF(in, 2);
            sprintf(in->cur, " )");
            in->cur = strchr(in->cur, '\0');
        }
    }
    else
    {
        // Handle binary operators (AND, OR, PHRASE)
        int8 op = in->curpol->qoperator.oper;
        int priority = QO_PRIORITY(in->curpol);
        int16 distance = in->curpol->qoperator.distance;
        INFIX nrm;
        bool needParenthesis = false;

        in->curpol++;

        // Determine if parentheses are needed
        if (priority < parentPriority || (op == OP_PHRASE && rightPhraseOp))
        {
            needParenthesis = true;
            RESIZEBUF(in, 2);
            sprintf(in->cur, "( ");
            in->cur = strchr(in->cur, '\0');
        }

        // Set up buffer for right operand
        nrm.curpol = in->curpol;
        nrm.op = in->op;
        nrm.buflen = 16;
        nrm.cur = nrm.buf = (char *) palloc(sizeof(char) * nrm.buflen);

        // Format right operand first
        infix(&nrm, priority, (op == OP_PHRASE));

        // Format left operand
        in->curpol = nrm.curpol;
        infix(in, priority, false);

        // Add operator symbol and right operand
        RESIZEBUF(in, 3 + (2 + 10) + (nrm.cur - nrm.buf));
        switch (op)
        {
            case OP_OR:
                sprintf(in->cur, " | %s", nrm.buf);
                break;
            case OP_AND:
                sprintf(in->cur, " & %s", nrm.buf);
                break;
            case OP_PHRASE:
                if (distance != 1)
                    sprintf(in->cur, " <%d> %s", distance, nrm.buf);
                else
                    sprintf(in->cur, " <-> %s", nrm.buf);
                break;
            default:
                elog(ERROR, "unrecognized operator type: %d", op);
        }

        in->cur = strchr(in->cur, '\0');
        pfree(nrm.buf);

        // Close parentheses if needed
        if (needParenthesis)
        {
            RESIZEBUF(in, 2);
            sprintf(in->cur, " )");
            in->cur = strchr(in->cur, '\0');
        }
    }
}
```