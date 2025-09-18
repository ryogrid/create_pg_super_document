# add_variable_to_head

## Location
src/interfaces/ecpg/preproc/variable.c: 377 - 388

## Overview
Inserts a new variable and its optional indicator variable at the beginning of an argument list, implementing a LIFO (Last In, First Out) insertion strategy.

## Definition


## Detailed Description
The  function adds a new entry to the beginning of a doubly-linked list of arguments. This function is part of PostgreSQL's ECPG preprocessor and is used to build lists of variables that will be used as parameters or result holders in SQL statements. The function creates a new  node containing the provided variable and its associated indicator variable, then inserts it at the head of the list.

The comment in the source code explains an important implementation detail: since the list is dumped from the end during code generation, adding new entries at the beginning ensures proper ordering in the final output. This LIFO behavior is intentional and matches the expected processing order.

## Parameters / Member Variables
- : Double pointer to the head of the arguments list; allows modification of the list head pointer
- : Pointer to the main variable to be added to the list 
- : Pointer to the indicator variable associated with the main variable (can be NULL if no indicator is needed)

## Dependencies
- Functions called/Symbols referenced:
  - : Memory allocation function used to create new argument nodes
  - : The node structure for the linked list
  - : Structure representing ECPG variables
- Called from (representative examples):
  - Various locations in ECPG grammar rules (ecpg.trailer)
  - Used for both  and  global lists

## Notes and Other Information
- The function uses  for memory allocation, which is ECPG's memory management system
- The LIFO insertion order is crucial for proper code generation - the list is processed from end to beginning
- Both the main variable and indicator can be passed; indicator variables are used for NULL handling in ECPG
- The function modifies the list head pointer through the double pointer parameter
- No error checking is performed on the memory allocation, following ECPG's memory management conventions