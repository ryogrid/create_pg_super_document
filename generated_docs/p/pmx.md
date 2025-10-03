# pmx

## Location
[src/backend/optimizer/geqo/geqo_pmx.c:49-220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/geqo/geqo_pmx.c#L49-L220)

## Overview
The  function implements the Partially Matched Crossover (PMX) genetic algorithm operator for the GEQO (Genetic Query Optimizer) in PostgreSQL, used to create offspring solutions by combining two parent tour solutions.

## Definition

```c
void
pmx(PlannerInfo *root, Gene *tour1, Gene *tour2, Gene *offspring, int num_gene)
```
## Detailed Description
The PMX (Partially Matched Crossover) function is a sophisticated genetic crossover operator that creates a new offspring tour by combining genetic material from two parent tours while maintaining the validity of the solution. This algorithm is specifically designed for permutation-based genetic algorithms where each gene must appear exactly once in the solution.

The algorithm works in three main steps:
1. **Initial Setup**: Randomly selects crossover points and copies one parent (tour2) to the offspring
2. **Primary Crossover**: Replaces the segment between crossover points with material from the other parent (tour1)
3. **Conflict Resolution**: Handles duplicates and missing genes through a sophisticated mapping and replacement mechanism

The function ensures that the resulting offspring is a valid permutation where each gene appears exactly once, making it suitable for optimization problems like query join order optimization.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner context and random number generation state
- `*tour1`: First parent tour (gene sequence) - acts as the "MOM" in the crossover
- `*tour2`: Second parent tour (gene sequence) - acts as the "DAD" in the crossover
- `*offspring`: Output array where the resulting offspring tour will be stored
- `num_gene`: Number of genes in each tour (length of the gene sequences)
## Dependencies
- Functions called/Symbols referenced:
  - : Memory allocation for internal arrays
  - : Memory deallocation for cleanup
  - : Random integer generation for crossover point selection
  - : Type definition for individual genes in the tour
  - , : Constants used to track gene origin during crossover
- Called from (representative examples):
  - : Main GEQO algorithm function in geqo_main.c:198

## Notes and Other Information
- The PMX algorithm is particularly well-suited for permutation problems where order matters and no duplicates are allowed
- Uses a three-step process with sophisticated conflict resolution to maintain solution validity
- Allocates temporary arrays (, , , ) for tracking gene mappings and conflicts
- The crossover points are selected randomly, with proper handling when left > right
- Part of PostgreSQL's genetic query optimization system, used to optimize complex multi-table joins
- Memory management is handled properly with palloc/pfree pairs for all temporary allocations
- The algorithm maintains a check_list to ensure each gene appears exactly once in the final offspring

## Simplified Source

```c
void
pmx(PlannerInfo *root, Gene *tour1, Gene *tour2, Gene *offspring, int num_gene)
{
    int *failed = (int *) palloc((num_gene + 1) * sizeof(int));
    int *from = (int *) palloc((num_gene + 1) * sizeof(int));
    int *indx = (int *) palloc((num_gene + 1) * sizeof(int));
    int *check_list = (int *) palloc((num_gene + 1) * sizeof(int));

    int left, right, temp, i, j, k;
    int mx_fail, found, mx_hold;

    // Initialize tracking arrays
    for (k = 0; k < num_gene; k++)
    {
        failed[k] = -1;
        from[k] = -1;
        check_list[k + 1] = 0;
    }

    // Select random crossover points
    left = geqo_randint(root, num_gene - 1, 0);
    right = geqo_randint(root, num_gene - 1, 0);

    if (left > right)
    {
        temp = left;
        left = right;
        right = temp;
    }

    // Copy tour2 to offspring as base
    for (k = 0; k < num_gene; k++)
    {
        offspring[k] = tour2[k];
        from[k] = DAD;
        check_list[tour2[k]]++;
    }

    // Replace crossover segment with tour1
    for (k = left; k <= right; k++)
    {
        check_list[offspring[k]]--;
        offspring[k] = tour1[k];
        from[k] = MOM;
        check_list[tour1[k]]++;
    }

    // Resolve conflicts (3-step process)
    mx_fail = 0;

    // Step 1: Try direct replacements
    for (k = left; k <= right; k++)
    {
        if (tour1[k] != tour2[k])
        {
            found = 0;
            for (j = 0; j < num_gene && !found; j++)
            {
                if ((offspring[j] == tour1[k]) && (from[j] == DAD))
                {
                    check_list[offspring[j]]--;
                    offspring[j] = tour2[k];
                    check_list[tour2[k]]++;
                    found = 1;
                }
            }

            if (!found)
            {
                failed[mx_fail] = (int) tour1[k];
                indx[mx_fail] = k;
                mx_fail++;
            }
        }
    }

    // Step 2: Handle failed replacements
    if (mx_fail > 0)
    {
        for (k = 0; k < mx_fail; k++)
        {
            for (j = 0; j < num_gene; j++)
            {
                if ((failed[k] == (int) offspring[j]) && (from[j] == DAD))
                {
                    check_list[offspring[j]]--;
                    offspring[j] = tour2[indx[k]];
                    check_list[tour2[indx[k]]]++;
                    break;
                }
            }
        }
    }

    // Step 3: Fix remaining duplicates
    for (k = 1; k <= num_gene; k++)
    {
        if (check_list[k] > 1)
        {
            for (i = 0; i < num_gene; i++)
            {
                if ((offspring[i] == (Gene) k) && (from[i] == DAD))
                {
                    for (j = 1; j <= num_gene; j++)
                    {
                        if (check_list[j] == 0)
                        {
                            offspring[i] = (Gene) j;
                            check_list[k]--;
                            check_list[j]++;
                            break;
                        }
                    }
                    break;
                }
            }
        }
    }

    // Clean up memory
    pfree(failed);
    pfree(from);
    pfree(indx);
    pfree(check_list);
}
```