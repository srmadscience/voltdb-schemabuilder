# voltdb-schemabuilder

This is a utility to create and alter the DDL and code needed to run a VoltDB application programmatically. 

It has a constructor that takes the following parameters:

 | Parameter | Purpose |
 | ---       | ---     | 
 |ddlStatements | Array containing DDL statements in a logical order. | 
procStatements| Array containing procedure definitions|
zipFiles |Array containing names of zip files we want to include
jarFileName| name of Jar file we create|
 voltClient |handle to Volt Client. Note we never close it, as we assume somebody else needs it for something...|
procPackageName| Java package our stored procedures are in|
testProcName | name of test procedure. We test to see whether the schema exists by calling a (hopefully) read only procedure |
testParam| Array of parameters for  the above test procedure|
otherClasses | Other java classes we need to load|

You then create the schema by calling ````loadClassesAndDDLIfNeeded()```` .
