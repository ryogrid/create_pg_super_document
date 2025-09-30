# UserAbortTransactionBlock\n\n## Overview\nUserAbortTransactionBlock executes a ROLLBACK command by transitioning the transaction state machine to appropriate abort states based on the current transaction context.\n\n## Definition\nvoid UserAbortTransactionBlock(bool chain)\n\n## Detailed Description\nUserAbortTransactionBlock handles user-initiated transaction rollbacks through the ROLLBACK SQL command. Rather than performing actual abort processing, it manages the transaction state machine by setting appropriate block states that signal to CommitTransactionCommand() what actions to take. The function handles various transaction contexts including regular transactions, failed transactions, subtransactions, implicit transactions, and parallel operations. When processing subtransactions, it walks up the transaction stack to mark all levels for abort. The chain parameter controls whether the rollback should start a new transaction (ROLLBACK AND CHAIN).\n\n## Parameters / Member Variables\n- chain: Boolean flag indicating whether to start a new transaction after rollback (true for ROLLBACK AND CHAIN, false for plain ROLLBACK)\n\n## Dependencies\n- Functions called/Symbols referenced:\n  - CurrentTransactionState (global variable)\n  - [BlockStateAsString](../B/BlockStateAsString.md)\n  - ereport\n  - elog\n  - Assert\n  - Various TBLOCK_* state constants\n- Called from (representative examples):\n  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)\n\n## Notes and Other Information\n- The function only changes blockState values and does not perform actual transaction abort processing\n- For subtransactions, it marks all levels in the stack for abort by walking up to the top-level transaction\n- Issues warnings when ROLLBACK is used outside transaction blocks, but allows the operation to proceed\n- ROLLBACK AND CHAIN is prohibited outside transaction blocks and generates an error\n- Parallel worker contexts cannot handle abort operations and result in FATAL errors\n- The function ensures the final state is either TBLOCK_ABORT_END or TBLOCK_ABORT_PENDING

## Simplified Source

```c
void UserAbortTransactionBlock(bool chain) {
    TransactionState s = CurrentTransactionState;

    switch (s->blockState) {
        // Normal transaction in progress - mark for abort
        case TBLOCK_INPROGRESS:
            s->blockState = TBLOCK_ABORT_PENDING;
            break;

        // Already failed transaction - just mark for cleanup
        case TBLOCK_ABORT:
            s->blockState = TBLOCK_ABORT_END;
            break;

        // Subtransaction - mark all levels up to top for abort
        case TBLOCK_SUBINPROGRESS:
        case TBLOCK_SUBABORT:
            while (s->parent != NULL) {
                if (s->blockState == TBLOCK_SUBINPROGRESS)
                    s->blockState = TBLOCK_SUBABORT_PENDING;
                else if (s->blockState == TBLOCK_SUBABORT)
                    s->blockState = TBLOCK_SUBABORT_END;
                s = s->parent;
            }
            // Handle top-level transaction
            if (s->blockState == TBLOCK_INPROGRESS)
                s->blockState = TBLOCK_ABORT_PENDING;
            else if (s->blockState == TBLOCK_ABORT)
                s->blockState = TBLOCK_ABORT_END;
            break;

        // Not in transaction - warn or error based on chain flag
        case TBLOCK_STARTED:
        case TBLOCK_IMPLICIT_INPROGRESS:
            if (chain) {
                // ROLLBACK AND CHAIN requires transaction block
                ereport(ERROR, "ROLLBACK AND CHAIN can only be used in transaction blocks");
            } else {
                // Plain ROLLBACK - just warn and proceed
                ereport(WARNING, "there is no transaction in progress");
            }
            s->blockState = TBLOCK_ABORT_PENDING;
            break;

        // Cannot abort in parallel worker
        case TBLOCK_PARALLEL_INPROGRESS:
            ereport(FATAL, "cannot abort during a parallel operation");
            break;

        // All other states are invalid for user abort
        default:
            elog(FATAL, "UserAbortTransactionBlock: unexpected state");
            break;
    }

    s->chain = chain;
}
```