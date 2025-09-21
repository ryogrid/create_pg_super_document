34.16. Oracle Compatibility Mode  
---  
[Prev](ecpg-informix-compat.md "34.15. Informix Compatibility Mode") | [Up](ecpg.md "Chapter 34. ECPG — Embedded SQL in C")| Chapter 34. ECPG — Embedded SQL in C| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ecpg-develop.md "34.17. Internals")  
  
* * *

## 34.16. Oracle Compatibility Mode #

`ecpg` can be run in a so-called _Oracle compatibility mode_. If this mode is active, it tries to behave as if it were Oracle Pro*C. 

Specifically, this mode changes `ecpg` in three ways: 

  * Pad character arrays receiving character string types with trailing spaces to the specified length 

  * Zero byte terminate these character arrays, and set the indicator variable if truncation occurs 

  * Set the null indicator to `-1` when character arrays receive empty character string types 




* * *

[Prev](ecpg-informix-compat.md "34.15. Informix Compatibility Mode") | [Up](ecpg.md "Chapter 34. ECPG — Embedded SQL in C")|  [Next](ecpg-develop.md "34.17. Internals")  
---|---|---  
34.15. Informix Compatibility Mode | [Home](index.md "PostgreSQL 17.5 Documentation")|  34.17. Internals
