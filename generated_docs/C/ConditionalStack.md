# ConditionalStack

## Location
[src/include/fe_utils/conditional.h:71-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/conditional.h#L71-L102)

## Overview
ConditionalStack is a type alias for ConditionalStackData pointer that provides the public interface for managing nested conditional blocks in PostgreSQL frontend utilities.

## Definition


## Detailed Description
ConditionalStack serves as the main public interface for conditional processing in PostgreSQL command-line tools like psql and pgbench. It is a pointer type that encapsulates a ConditionalStackData structure, providing a clean abstraction for managing nested \if...\endif blocks. The type comes with a comprehensive API that handles stack operations, state management, and query buffer coordination necessary for proper conditional execution.

## Parameters / Member Variables
This is a type alias for a pointer to ConditionalStackData, so it has no direct members. It points to a structure containing:
- Indirect access to IfStackElem linked list through ConditionalStackData->head

## Dependencies
- Functions called/Symbols referenced:
  - [ConditionalStackData](ConditionalStackData.md) (underlying data structure)
  - ifState (enum for conditional states)
  - [conditional_stack_create](../c/conditional_stack_create.md) (constructor)
  - [conditional_stack_destroy](../c/conditional_stack_destroy.md) (destructor)
  - [conditional_stack_push](../c/conditional_stack_push.md) (add new conditional level)
  - [conditional_stack_pop](../c/conditional_stack_pop.md) (remove conditional level)
  - [conditional_stack_peek](../c/conditional_stack_peek.md) (examine top state)
  - [conditional_stack_poke](../c/conditional_stack_poke.md) (modify top state)
  - [conditional_stack_empty](../c/conditional_stack_empty.md) (check if stack is empty)
  - [conditional_active](../c/conditional_active.md) (check if currently in active branch)
  - [conditional_stack_set_query_len](../c/conditional_stack_set_query_len.md)/conditional_stack_get_query_len (query buffer management)
  - [conditional_stack_set_paren_depth](../c/conditional_stack_set_paren_depth.md)/conditional_stack_get_paren_depth (lexer state management)
- Called from (representative examples):
  - psql command processing (exec_command_if, exec_command_elif, exec_command_else, exec_command_endif)
  - pgbench conditional processing (CheckConditional)
  - [Query](../Q/Query.md) text management (save_query_text_state, discard_query_text)

## Notes and Other Information
This type provides a complete API for conditional processing in PostgreSQL frontend tools. The interface supports full conditional logic including nested if-elif-else-endif constructs, proper query buffer management for discarding inactive branches, and lexer state preservation. The implementation is used extensively in both psql and pgbench for interactive conditional execution of commands and scripts.