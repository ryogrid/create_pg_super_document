67.6. BKI Example  
---  
[Prev](bki-structure.md "67.5. Structure of the Bootstrap BKI File") | [Up](bki.md "Chapter 67. System Catalog Declarations and Initial Contents")| Chapter 67. System Catalog Declarations and Initial Contents| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](planner-stats-details.md "Chapter 68. How the Planner Uses Statistics")  
  
* * *

## 67.6. BKI Example #

The following sequence of commands will create the table `test_table` with OID 420, having three columns `oid`, `cola` and `colb` of type `oid`, `int4` and `text`, respectively, and insert two rows into the table: 
    
    
    create test_table 420 (oid = oid, cola = int4, colb = text)
    open test_table
    insert ( 421 1 'value 1' )
    insert ( 422 2 _null_ )
    close test_table
    

* * *

[Prev](bki-structure.md "67.5. Structure of the Bootstrap BKI File") | [Up](bki.md "Chapter 67. System Catalog Declarations and Initial Contents")|  [Next](planner-stats-details.md "Chapter 68. How the Planner Uses Statistics")  
---|---|---  
67.5. Structure of the Bootstrap BKI File | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 68. How the Planner Uses Statistics
