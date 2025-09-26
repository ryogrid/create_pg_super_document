# Gene

## Location
src/include/optimizer/geqo_gene.h: 30 - 31

## Overview
A typedef representing a single gene in PostgreSQL's Genetic Query Optimizer (GEQO), which is an integer value used to represent a relation identifier in genetic algorithm chromosomes.

## Definition

```c
typedef int Gene;
```
## Detailed Description
Gene is a fundamental data type in PostgreSQL's Genetic Query Optimizer (GEQO) that represents a single gene within a chromosome. Each gene corresponds to a relation (table) identifier in the query optimization context. The GEQO module uses genetic algorithms to solve complex join ordering problems, and genes are the atomic units that make up the chromosomes representing different join orders.

The comment in the source code indicates that using  instead of  was a deliberate design choice for the GEQO implementation, and this should not be changed. This typedef provides abstraction and clarity in the genetic algorithm code, making it clear when variables represent genetic algorithm genes versus regular integers.

## Parameters / Member Variables
This is a simple typedef with no members or parameters - it aliases the  type.

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a basic typedef of int)
- Called from (representative examples):
  - cx (cycle crossover operation)
  - gimme_edge_table (edge recombination functions)
  - gimme_tour
  - remove_gene
  - gimme_gene
  - edge_failure
  - geqo_eval
  - gimme_tree
  - geqo (main genetic algorithm function)
  - geqo_mutation
  - ox1, ox2 (order crossover operations)
  - pmx (partially mapped crossover)
  - alloc_pool
  - alloc_chromo
  - px (position crossover)
  - init_tour

## Notes and Other Information
- The Gene typedef is explicitly designed to use  rather than  for compatibility and performance reasons in the genetic algorithm implementation
- The source comment warns against changing this typedef, indicating it's a critical design decision
- Genes are typically arranged in arrays (strings) within Chromosome structures to represent complete join orders
- The genetic algorithm operations (crossover, mutation, selection) work on sequences of these Gene values
- Each Gene value represents a relation ID that corresponds to a table in the query being optimized
- The GEQO module uses various genetic operators (PMX, OX1, OX2, ERX, etc.) that manipulate arrays of Gene values
- This abstraction allows the genetic algorithm code to be more readable and maintainable by clearly distinguishing genetic algorithm genes from other integer values in the codebase