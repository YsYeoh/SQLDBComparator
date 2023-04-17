@ECHO OFF

SET PATH=%cd%\dist\SqlComparator.jar

SET MSSQL_CONN=jdbc:sqlserver://localhost:1433;databaseName=RBGCLIENT;trustServerCertificate=true;
SET MSSQL_USER=rbguser
SET MSSQL_PWD=rbgu53r

SET PGSQL_CONN=jdbc:postgresql://localhost:5432/RBGCLIENT
SET PGSQL_USER=rbguser
SET PGSQL_PWD=rbgu53r

"%JAVA_HOME%\bin\java" -jar "%PATH%" %MSSQL_CONN% %MSSQL_USER% %MSSQL_PWD% %PGSQL_CONN% %PGSQL_USER% %PGSQL_PWD%

pause