# SQLDBComparator
A pure Java Ant program and it is a database comparator for MSSQL and PostgreSQL to compare table, column, and data inside both database, help people to validate their db migration correctness.

Configuration Step
1. Download the source file
2. Open and Edit "SqlComparator.bat" file
3. Change following connection string and credentials for db instance connection purpose
4. SET MSSQL_CONN=jdbc:sqlserver://<hostname>:<port>;databaseName=<dbname>;trustServerCertificate=true;
5. SET MSSQL_USER=<db username>
6. SET MSSQL_PWD=<db password>
7. SET PGSQL_CONN=jdbc:postgresql://<hostname>:<port>/<dbname>
8. SET PGSQL_USER=<db username>
9. SET PGSQL_PWD=<db password>
10. Run "SqlComparator.bat"
11. server.log file consist the report, or you may look at the command prompt windows.
***Note that "trustServerCertificate=true" is optional, if no SSL cert, then set to true is mandatory*** 
