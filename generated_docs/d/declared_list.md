# declared_list

## Location
[src/interfaces/ecpg/preproc/type.h:150-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.h#L150-L156)

## Overview
The `declared_list` struct maintains a linked list of declared statements or entities in PostgreSQL's ECPG preprocessor, tracking names and their associated database connections.

## Definition
```c
struct declared_list
{
    char           *name;
    char           *connection;
    struct declared_list *next;
};
```

## Detailed Description
This structure is used by the ECPG preprocessor to maintain a list of declared entities (such as prepared statements or cursors) along with their associated database connections. It implements a simple linked list data structure that allows the preprocessor to track and manage multiple declarations throughout the compilation process.

## Parameters / Member Variables
- `name`: Pointer to the name string of the declared entity
- `connection`: Pointer to the database connection name string associated with this declaration
- `next`: Pointer to the next declared_list node in the linked list

## Dependencies
- Functions called/Symbols referenced:
  - declared_list (self-reference for linked list structure)
- Called from (representative examples):
  - main (in src/interfaces/ecpg/preproc/ecpg.c:365, 395)

## Notes and Other Information
- This structure is part of the ECPG preprocessor implementation (src/interfaces/ecpg/preproc/type.h:150-155)
- Implements a simple singly-linked list for managing declaration tracking
- Used by the main function in the ECPG preprocessor for managing declared entities
- Provides connection-aware declaration management for embedded SQL preprocessing