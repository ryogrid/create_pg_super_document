# print_pathkeys

## Location
[src/backend/nodes/print.c:426-465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/print.c#L426-L465)

## Overview
A debugging utility function that prints a formatted representation of pathkeys, which are used in PostgreSQL's query planner to represent sort ordering requirements.

## Definition

```c
void
print_pathkeys(const List *pathkeys, const List *rtable)
```
## Detailed Description
The  function provides a human-readable output of pathkeys, which are essential data structures in PostgreSQL's query optimizer. Pathkeys represent sort ordering requirements and are used to determine whether one path's output can satisfy another operation's ordering needs without additional sorting.

The function iterates through a list of PathKey structures, extracting and displaying the equivalence class members for each pathkey. For each equivalence class, it handles merged classes by chasing up to the canonical representative and then prints all member expressions within that class.

The output format uses parentheses to group related items, with comma separation between multiple pathkeys and between equivalence class members.

## Parameters / Member Variables
- `*pathkeys`: A List of PathKey pointers representing the sort ordering requirements to be printed
- `*rtable`: A List representing the range table, used to provide context for expression printing
## Dependencies
- Functions called/Symbols referenced:
  - [PathKey](../P/PathKey.md) (structure type)
  - [EquivalenceClass](../E/EquivalenceClass.md) (structure type)  
  - [EquivalenceMember](../E/EquivalenceMember.md) (structure type)
  - [print_expr](print_expr.md) (function to print individual expressions)
  - [lnext](../l/lnext.md) (list navigation function)
- Called from (representative examples):
  - nodeDisplay (via print.h header inclusion)

## Notes and Other Information
- This is primarily a debugging function used for development and troubleshooting query planning issues
- The function handles the case where equivalence classes have been merged by following the ec_merged chain to find the canonical representative
- Output is sent directly to stdout via printf statements
- Located in src/backend/nodes/print.c, part of PostgreSQL's node printing utilities

## Simplified Source

```c
void print_pathkeys(const List *pathkeys, const List *rtable) {
    const ListCell *i;

    printf("(");
    foreach(i, pathkeys) {
        PathKey *pathkey = (PathKey *) lfirst(i);
        EquivalenceClass *eclass = pathkey->pk_eclass;

        // Follow merged equivalence classes to canonical representative
        while (eclass->ec_merged)
            eclass = eclass->ec_merged;

        // Print all members of this equivalence class
        printf("(");
        ListCell *k;
        bool first = true;
        foreach(k, eclass->ec_members) {
            EquivalenceMember *mem = (EquivalenceMember *) lfirst(k);

            if (first)
                first = false;
            else
                printf(", ");

            print_expr((Node *) mem->em_expr, rtable);
        }
        printf(")");

        if (lnext(pathkeys, i))
            printf(", ");
    }
    printf(")\n");
}
```