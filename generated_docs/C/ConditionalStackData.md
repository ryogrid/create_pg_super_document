# ConditionalStackData

## Location
[src/include/fe_utils/conditional.h:66-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/conditional.h#L66-L69)

## Overview
ConditionalStackData is a struct that serves as the main container for managing a stack of conditional states in PostgreSQL frontend utilities.

## Definition

```c
typedef struct ConditionalStackData
{
	IfStackElem *head;
}			ConditionalStackData;
```
## Detailed Description
ConditionalStackData acts as the primary data structure for managing nested conditional blocks (\if...\endif) in PostgreSQL command-line tools like psql and pgbench. It maintains a simple linked list stack where each node represents a level of conditional nesting. The structure provides a clean interface for conditional stack operations while encapsulating the underlying IfStackElem linked list implementation.

## Parameters / Member Variables
- `*head`: Pointer to the top IfStackElem in the conditional stack, representing the most recently entered (innermost) conditional block; NULL when no conditionals are active
## Dependencies
- Functions called/Symbols referenced:
  - [IfStackElem](../I/IfStackElem.md) (struct representing individual stack elements)
- Called from (representative examples):
  - [conditional_stack_create](../c/conditional_stack_create.md)
  - [ConditionalStack](ConditionalStack.md) (type alias)

## Notes and Other Information
This structure serves as the foundation for conditional processing in PostgreSQL frontend tools. It provides a clean abstraction over the underlying linked list implementation, allowing for easy stack operations while maintaining the state information necessary for proper conditional block execution and query buffer management.