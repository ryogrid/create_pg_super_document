Chapter 68. How the Planner Uses Statistics  
---  
[Prev](bki-example.md "67.6. BKI Example") | [Up](internals.md "Part VII. Internals")| Part VII. Internals| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](row-estimation-examples.md "68.1. Row Estimation Examples")  
  
* * *

## Chapter 68. How the Planner Uses Statistics

**Table of Contents**

[68.1. Row Estimation Examples](row-estimation-examples.md)
[68.2. Multivariate Statistics Examples](multivariate-statistics-examples.md)
    

[68.2.1. Functional Dependencies](multivariate-statistics-examples.md#FUNCTIONAL-DEPENDENCIES)
[68.2.2. Multivariate N-Distinct Counts](multivariate-statistics-examples.md#MULTIVARIATE-NDISTINCT-COUNTS)
[68.2.3. MCV Lists](multivariate-statistics-examples.md#MCV-LISTS)
[68.3. Planner Statistics and Security](planner-stats-security.md)

This chapter builds on the material covered in [Section 14.1](using-explain.md "14.1. Using EXPLAIN") and [Section 14.2](planner-stats.md "14.2. Statistics Used by the Planner") to show some additional details about how the planner uses the system statistics to estimate the number of rows each part of a query might return. This is a significant part of the planning process, providing much of the raw material for cost calculation. 

The intent of this chapter is not to document the code in detail, but to present an overview of how it works. This will perhaps ease the learning curve for someone who subsequently wishes to read the code. 

* * *

[Prev](bki-example.md "67.6. BKI Example") | [Up](internals.md "Part VII. Internals")|  [Next](row-estimation-examples.md "68.1. Row Estimation Examples")  
---|---|---  
67.6. BKI Example | [Home](index.md "PostgreSQL 17.5 Documentation")|  68.1. Row Estimation Examples
