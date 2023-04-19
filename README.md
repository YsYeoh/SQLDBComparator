# SQLDBComparator
A pure Java Ant program and it is a database comparator for MSSQL and PostgreSQL to compare table, column, and data inside both database, help people to validate their db migration correctness.

Configuration Step
1. Download the source file
2. Open and Edit "SqlComparator.bat" file
3. Change following connection string and credentials for db instance connection purpose

    SET MSSQL_CONN=jdbc:sqlserver://<hostname>:<port>;databaseName=<dbname>;trustServerCertificate=true;
    SET MSSQL_USER=<db username>
    SET MSSQL_PWD=<db password>

    SET PGSQL_CONN=jdbc:postgresql://<hostname>:<port>/<dbname>
    SET PGSQL_USER=<db username>
    SET PGSQL_PWD=<db password>
  
  ***Note that "trustServerCertificate=true" is optional, if no SSL cert, then set to true is mandatory*** 

4. Run "SqlComparator.bat"
5. server.log file consist the report, or you may look at the command prompt windows.
