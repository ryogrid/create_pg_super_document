19.16. Customized Options  
---  
[Prev](runtime-config-preset.md "19.15. Preset Options") | [Up](runtime-config.md "Chapter 19. Server Configuration")| Chapter 19. Server Configuration| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](runtime-config-developer.md "19.17. Developer Options")  
  
* * *

## 19.16. Customized Options #

This feature was designed to allow parameters not normally known to PostgreSQL to be added by add-on modules (such as procedural languages). This allows extension modules to be configured in the standard ways. 

Custom options have two-part names: an extension name, then a dot, then the parameter name proper, much like qualified names in SQL. An example is `plpgsql.variable_conflict`. 

Because custom options may need to be set in processes that have not loaded the relevant extension module, PostgreSQL will accept a setting for any two-part parameter name. Such variables are treated as placeholders and have no function until the module that defines them is loaded. When an extension module is loaded, it will add its variable definitions and convert any placeholder values according to those definitions. If there are any unrecognized placeholders that begin with its extension name, warnings are issued and those placeholders are removed. 

* * *

[Prev](runtime-config-preset.md "19.15. Preset Options") | [Up](runtime-config.md "Chapter 19. Server Configuration")|  [Next](runtime-config-developer.md "19.17. Developer Options")  
---|---|---  
19.15. Preset Options | [Home](index.md "PostgreSQL 17.5 Documentation")|  19.17. Developer Options
