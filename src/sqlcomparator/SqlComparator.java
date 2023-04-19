/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package sqlcomparator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;
 
/**
 *
 * @author Yong Sen & Jensen
 */
public class SqlComparator {

    /**
     * @param args the command line arguments
     */
    
    private static String MSSQL_CONNECTION_STRING;
    private static String MSSQL_USER;
    private static String MSSQL_PWD;
    
    private static String PGSQL_CONNECTION_STRING;
    private static String PGSQL_USER;
    private static String PGSQL_PWD;
    
    private static String NEXT_LINE = System.lineSeparator();
    private static String TAB = "  ";
    private static String BREAK_LINE = "=========================================================================================================================";
    
    public SqlComparator(){
    }
    
    public static void main(String[] args) {
        // TODO code application logic here
        
        if(!setParams(args)){
            
            fileWritter("Parameter missing, please check the batch file");
            
            return;
        }
        
        Map<String, Integer> countOfTableResultMap = getCountOfTable();
        Map<String, String> tableNameResultMap = getTableName();
        
        StringBuilder output = new StringBuilder();
        
        boolean compareNumberOfTable = countOfTableResultMap.get("MSSQL_Num_Of_Table").equals(countOfTableResultMap.get("PGSQL_Num_Of_Table"));
        boolean compareTableName = tableNameResultMap.get("MSSQL_Table_Name").equals(tableNameResultMap.get("PGSQL_Table_Name"));
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date dateStart = new Date();

        output.append("Run SqlComparator: " + dateFormat.format(dateStart));
        output.append(NEXT_LINE);
        output.append(NEXT_LINE);
        output.append("Database Migration Validation");
        output.append(NEXT_LINE);
        output.append(BREAK_LINE);
        output.append(NEXT_LINE);
        output.append(padRight("No.", 5));
        output.append(padRight("PGSQL Number Of Table", 30));
        output.append(padRight("MSSQL Number Of Table", 30));
        output.append(NEXT_LINE);
        output.append(padRight("1", 5));
        output.append(padRight(countOfTableResultMap.get("MSSQL_Num_Of_Table")+"", 30));
        output.append(padRight(countOfTableResultMap.get("PGSQL_Num_Of_Table")+"", 30));
        output.append(NEXT_LINE);
        output.append(BREAK_LINE);
        output.append(NEXT_LINE);
        output.append("Table Count Comparison Result: " + compareNumberOfTable);
        output.append(NEXT_LINE);
        output.append("Table Name Comparison Result: " + compareTableName);
        output.append(NEXT_LINE);
        output.append("Overall: " + (compareNumberOfTable && compareTableName));
        output.append(NEXT_LINE);
        output.append(NEXT_LINE);
        
        fileWritter(output.toString());
        //IF Table migration no issue, we only start to compare different between each table
        if(compareNumberOfTable && compareTableName){

            //IF column migration no issue, we only start to compare data migration
            if(compareTableColumn()){

                if(compareTableRow()){

                    compareTableData();
                    displayEnding();
                
                }
                else{
                    fileWritter("Number of row in some table is different, please make sure data is migrate correctly before proceed to others checking!");
                    displayEnding();
                }
            }
            else{
                fileWritter("Number of Column or Column name in two database is not same, please make sure column in all table is migrate correctly before proceed to others checking!");
                displayEnding();
            }
        }
        else{
            fileWritter("Number of table OR table name in two database is not same, please make sure table is migrate correctly before proceed to others checking!");
            displayEnding();
        }   
    }

    private static boolean setParams(String[] args){
        boolean status = false;
        if(args.length == 6){
            MSSQL_CONNECTION_STRING = args[0];
            MSSQL_USER = args[1];
            MSSQL_PWD = args[2];
            PGSQL_CONNECTION_STRING = args[3];
            PGSQL_USER = args[4];
            PGSQL_PWD = args[5];
            
            status = true;
        }
        return status;
    }
    
    private static Map<String, Integer> getCountOfTable(){
        
        String countTableQuery = "SELECT COUNT(*) AS total FROM INFORMATION_SCHEMA.tables WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'";
        
        Map<String, Integer> resultMap = new HashMap<>();
        
        try (Connection sqlConnection = DriverManager.getConnection(MSSQL_CONNECTION_STRING, MSSQL_USER, MSSQL_PWD)) {
             // retrieve data from MS SQL Server database
            Statement sqlStatement = sqlConnection.createStatement();
            ResultSet sqlResultSet = sqlStatement.executeQuery(countTableQuery);
            
            if(sqlResultSet.next()){
                resultMap.put("MSSQL_Num_Of_Table", sqlResultSet.getInt("total"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        // establish connection to PostgreSQL database
        try (Connection pgConnection = DriverManager.getConnection(PGSQL_CONNECTION_STRING, PGSQL_USER, PGSQL_PWD)) {
            // retrieve data from PostgreSQL database
            Statement pgStatement = pgConnection.createStatement();
            ResultSet pgResultSet = pgStatement.executeQuery(countTableQuery);

            if(pgResultSet.next()){
                resultMap.put("PGSQL_Num_Of_Table", pgResultSet.getInt("total"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return resultMap;
    }
    
    private static Map<String, String> getTableName(){
        
        //Remove undercore since different SQL might have different mechanism of sorting
        String tableQuery = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.tables WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo' ORDER BY REPLACE(TABLE_NAME, '_', '') ASC";
        
        Map<String, String> resultMap = new HashMap<>();
        
        try (Connection sqlConnection = DriverManager.getConnection(MSSQL_CONNECTION_STRING, MSSQL_USER, MSSQL_PWD)) {
             // retrieve data from MS SQL Server database
            Statement sqlStatement = sqlConnection.createStatement();
            ResultSet sqlResultSet = sqlStatement.executeQuery(tableQuery);
            
            String strTable = "";
            while(sqlResultSet.next()){
                strTable += sqlResultSet.getString("table_name") + ",";
            }
            
            resultMap.put("MSSQL_Table_Name", strTable);
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        // establish connection to PostgreSQL database
        try (Connection pgConnection = DriverManager.getConnection(PGSQL_CONNECTION_STRING, PGSQL_USER, PGSQL_PWD)) {
            // retrieve data from PostgreSQL database
            Statement pgStatement = pgConnection.createStatement();
            ResultSet pgResultSet = pgStatement.executeQuery(tableQuery);

            String strTable = "";
            while(pgResultSet.next()){
                strTable += pgResultSet.getString("table_name") + ",";
            }
            
            resultMap.put("PGSQL_Table_Name", strTable);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return resultMap;
    }
    
    //Using PG DB
    private static ArrayList<String> getAllTableName(){
        ArrayList<String> tableArray = new ArrayList<String>();
        
        String tableQuery = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.tables WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo' ORDER BY REPLACE(TABLE_NAME, '_', '') ASC";
        
        // establish connection to PostgreSQL database
        //either using PG or MS to find all table
        try (Connection pgConnection = DriverManager.getConnection(PGSQL_CONNECTION_STRING, PGSQL_USER, PGSQL_PWD)) {
            // retrieve data from PostgreSQL database
            Statement pgStatement = pgConnection.createStatement();
            ResultSet pgResultSet = pgStatement.executeQuery(tableQuery);

            String strTable = "";
            while(pgResultSet.next()){
                tableArray.add(pgResultSet.getString("table_name"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return tableArray;
    }
    
    //Using PG DB
    private static ArrayList<String> getAllColunName(String table){
        ArrayList<String> tableArray = new ArrayList<String>();
        
        String tableQuery = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.tables WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo' ORDER BY REPLACE(TABLE_NAME, '_', '') ASC";
        
        // establish connection to PostgreSQL database
        //either using PG or MS to find all table
        try (Connection pgConnection = DriverManager.getConnection(PGSQL_CONNECTION_STRING, PGSQL_USER, PGSQL_PWD)) {
            // retrieve data from PostgreSQL database
            Statement pgStatement = pgConnection.createStatement();
            ResultSet pgResultSet = pgStatement.executeQuery(tableQuery);

            String strTable = "";
            while(pgResultSet.next()){
                tableArray.add(pgResultSet.getString("table_name"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return tableArray;
    }
    
    private static boolean compareTableRow(){
        ArrayList<String> tableArray = getAllTableName();
        
        boolean status = false;
        
        String query = "SELECT * FROM(";
        
        for(int i=0; i<tableArray.size(); i++){
            query += "SELECT '" + tableArray.get(i) + "' AS table_name, COUNT(*) as row_count FROM \"" + tableArray.get(i) + "\" UNION ALL ";
        }
        if(tableArray.size() > 0){
            query = query.substring(0, query.length()-10);
        }
        
        query += ") FINAL ORDER BY REPLACE(TABLE_NAME, '_', '') ASC";
        
        try (Connection sqlConnection = DriverManager.getConnection(MSSQL_CONNECTION_STRING, MSSQL_USER, MSSQL_PWD)) {
            
            // establish connection to PostgreSQL database
            try (Connection pgConnection = DriverManager.getConnection(PGSQL_CONNECTION_STRING, PGSQL_USER, PGSQL_PWD)) {
                
                // retrieve data from MSSQL database
                Statement sqlStatement = sqlConnection.createStatement();
                ResultSet sqlResultSet = sqlStatement.executeQuery(query);
                
                // retrieve data from PostgreSQL database
                Statement pgStatement = pgConnection.createStatement();
                ResultSet pgResultSet = pgStatement.executeQuery(query);

                String printResult = 
                        NEXT_LINE + NEXT_LINE +
                        "Data Count Validation" + NEXT_LINE +
                        BREAK_LINE + NEXT_LINE + 
                        padRight("No.", 5) +
                        padRight("TABLE_NAME", 50) + 
                        padRight("MSSQL", 10) + 
                        padRight("PGSQL", 10) + 
                        padRight("Data Count Comparison", 30) + 
                        //padRight("Row Name Comparison", 30) + 
                        NEXT_LINE + BREAK_LINE + NEXT_LINE;
                
                int i = 1;
                while(sqlResultSet.next() && pgResultSet.next()){
                    
                    if(sqlResultSet.getString("table_name").equals(pgResultSet.getString("table_name"))){
                        printResult += padRight( i + "", 5) +
                                        padRight(sqlResultSet.getString("table_name"), 50) + 
                                        padRight(sqlResultSet.getString("row_count"), 10) + 
                                        padRight(pgResultSet.getString("row_count"), 10) + 
                                        padRight(sqlResultSet.getString("row_count").equals(pgResultSet.getString("row_count"))+"", 30) + 
                                        //padRight(sqlResultSet.getString("column_str").equals(pgResultSet.getString("column_str"))+"", 30) +
                                        NEXT_LINE;
                        
                        if(sqlResultSet.getString("row_count").equals(pgResultSet.getString("row_count"))){
                            status = true;
                        }
                        else{
                            status = false;
                        }
                        
                        i++;
                    }
                }
                
                fileWritter(printResult);

            } catch (SQLException e) {
                e.printStackTrace();
            }
             // retrieve data from MS SQL Server database
            
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return status;
    }
    
    private static boolean compareTableColumn(){
        
        boolean status = false;

        String query = "SELECT C.TABLE_NAME as table_name, COUNT(C.COLUMN_NAME) as column_count, STRING_AGG(C.COLUMN_NAME, ',') as column_str " +
                        "FROM INFORMATION_SCHEMA.columns C, INFORMATION_SCHEMA.tables T " +
                        "WHERE C.TABLE_NAME = T.TABLE_NAME AND C.TABLE_SCHEMA = 'dbo' AND T.TABLE_TYPE = 'BASE TABLE' " +
                        "GROUP BY C.TABLE_NAME " +
                        "ORDER BY REPLACE(C.TABLE_NAME, '_', '') ASC";
        
        try (Connection sqlConnection = DriverManager.getConnection(MSSQL_CONNECTION_STRING, MSSQL_USER, MSSQL_PWD)) {
            
            // establish connection to PostgreSQL database
            try (Connection pgConnection = DriverManager.getConnection(PGSQL_CONNECTION_STRING, PGSQL_USER, PGSQL_PWD)) {
                
                // retrieve data from MSSQL database
                Statement sqlStatement = sqlConnection.createStatement();
                ResultSet sqlResultSet = sqlStatement.executeQuery(query);
                
                // retrieve data from PostgreSQL database
                Statement pgStatement = pgConnection.createStatement();
                ResultSet pgResultSet = pgStatement.executeQuery(query);

                String printResult = "Column Count and Migration Validation" + NEXT_LINE +
                        BREAK_LINE + NEXT_LINE + 
                        padRight("No.", 5) +
                        padRight("TABLE_NAME", 50) + 
                        padRight("MSSQL", 10) + 
                        padRight("PGSQL", 10) + 
                        padRight("Count Comparison", 20) + 
                        padRight("Column Name Comparison", 30) + 
                        NEXT_LINE + BREAK_LINE + NEXT_LINE;
                
                int i = 1;
                
                while(sqlResultSet.next() && pgResultSet.next()){
                    if(sqlResultSet.getString("table_name").equals(pgResultSet.getString("table_name"))){
                        printResult += padRight( i + "", 5) +
                                        padRight(sqlResultSet.getString("table_name"), 50) + 
                                        padRight(sqlResultSet.getString("column_count"), 10) + 
                                        padRight(pgResultSet.getString("column_count"), 10) + 
                                        padRight(sqlResultSet.getString("column_count").equals(pgResultSet.getString("column_count"))+"", 20) + 
                                        padRight(sqlResultSet.getString("column_str").equals(pgResultSet.getString("column_str"))+"", 30) +
                                        NEXT_LINE;
                        
                        i++;
                        
                        if(
                            sqlResultSet.getString("column_count").equals(pgResultSet.getString("column_count"))    
                            &&
                            sqlResultSet.getString("column_str").equals(pgResultSet.getString("column_str"))
                        ){
                            status = true;
                        }
                        else{
                            status = false;
                        }
                    }
                }
                
                fileWritter(printResult);

            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return status;
    }
    
    private static String getTablePK(String table){
        String pk = "";
        
        String query = "SELECT K.COLUMN_NAME as column_name, K.TABLE_NAME as table_name FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T " +
                        "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE K " +
                        "ON K.CONSTRAINT_NAME = T.CONSTRAINT_NAME " +
                        "AND K.TABLE_SCHEMA = 'dbo' " +
                        "AND T.CONSTRAINT_TYPE='PRIMARY KEY' " +
                        "AND K.TABLE_CATALOG = 'RBGCLIENT' " +
                        //"AND K.ORDINAL_POSITION = 1 " +
                        "AND K.TABLE_NAME = '" + table.toUpperCase() + "'";
        
        // establish connection to PostgreSQL database
        try (Connection pgConnection = DriverManager.getConnection(PGSQL_CONNECTION_STRING, PGSQL_USER, PGSQL_PWD)) {
             // retrieve data from PostgreSQL database
            Statement pgStatement = pgConnection.createStatement();
            ResultSet pgResultSet = pgStatement.executeQuery(query);

            while(pgResultSet.next()){
                pk += "\"" + pgResultSet.getString("column_name") + "\", ";
            }
            
            pk = pk.substring(0, pk.length()-2);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        
        return pk;
    }
    
    private static void compareTableData(){
        ArrayList<String> tableArray = getAllTableName();
        
        boolean status = false;
        
        String header = NEXT_LINE + "Data Migration Validation" + NEXT_LINE + BREAK_LINE + NEXT_LINE +
                        padRight("No.", 5) + 
                        padRight("TABLE_NAME", 50) +
                        padRight("RESULT", 10) +
                        padRight("Reason", 50) +
                        NEXT_LINE + BREAK_LINE + NEXT_LINE;
        
        fileWritter(header);
        
        for(int i = 0; i < tableArray.size(); i++){
            boolean unmatch = false;
            String printStr = padRight((i+1)+"", 5) + 
                            padRight(tableArray.get(i), 50);
            
            String reason = "";
            
            String pk = getTablePK(tableArray.get(i));
            String query = "SELECT * FROM \"" + tableArray.get(i) + "\" ORDER BY " + pk.toUpperCase() + " ASC";
            
            try (Connection sqlConnection = DriverManager.getConnection(MSSQL_CONNECTION_STRING, MSSQL_USER, MSSQL_PWD)) {
            
                // establish connection to PostgreSQL database
                try (Connection pgConnection = DriverManager.getConnection(PGSQL_CONNECTION_STRING, PGSQL_USER, PGSQL_PWD)) {

                    // retrieve data from MSSQL database
                    Statement sqlStatement = sqlConnection.createStatement();
                    ResultSet sqlResultSet = sqlStatement.executeQuery(query);

                    // retrieve data from PostgreSQL database
                    Statement pgStatement = pgConnection.createStatement();
                    ResultSet pgResultSet = pgStatement.executeQuery(query);

                    //Get Column Count
                    ResultSetMetaData sqlResultSetMetaData = sqlResultSet.getMetaData();
                    ResultSetMetaData pgResultSetMetaData = pgResultSet.getMetaData();
                    int sqlCount = sqlResultSetMetaData.getColumnCount();
                    int pgCount = pgResultSetMetaData.getColumnCount();
                    
                    int row = 1;
                    while(sqlResultSet.next() && pgResultSet.next()){
                        
                        if(sqlCount == pgCount){
                            
                            for(int j = 1; j <= sqlCount && !unmatch; j++){
                                
                                if(sqlResultSetMetaData.getColumnTypeName(j) == "xml" && pgResultSetMetaData.getColumnTypeName(j) == "xml"){
                                    String first = sqlResultSet.getString(j);
                                    String second = pgResultSet.getString(j);
                                    
                                    if(second != null && second.length()>0){
                                        second = second.substring(1);
                                    }
                                    
                                    if(first == null) first = "";
                                    if(second == null) second = "";
                                    
                                    if(!first.equals(second)){
                                        unmatch = true;
                                        reason = "Data unmatch in row " + row + " in column SQL:" + sqlResultSetMetaData.getColumnTypeName(j) + " Length:"  + first.length() + " PG:" + pgResultSetMetaData.getColumnTypeName(j) + " Length:"  + second.length();
                                    }
                                    
                                }
                                else{
                                    Object firstObj = sqlResultSet.getObject(j);
                                    Object secondObj = pgResultSet.getObject(j);

                                    if(!Objects.equals(firstObj, secondObj)){

                                        unmatch = true;
                                        reason = "Data unmatch in row " + row + " at column " + sqlResultSetMetaData.getColumnName(j) + ", MSSQL: " + firstObj.toString() + ", PGSQL: " + secondObj.toString();
                                    }
                                }
                            }
                        }
                        else{
                            reason = "Not able to compare data since column is column unmatch";
                        }
                        
                        row++;
                    }
                    

                } catch (SQLException e) {
                    e.printStackTrace();
                }
                 // retrieve data from MS SQL Server database


            } catch (SQLException e) {
                e.printStackTrace();
            }
            
            printStr += padRight(!unmatch+"", 10) +
                    padRight(reason, 50) + NEXT_LINE;
            
            
            fileWritter(printStr);
        }
    }
    
    private static String padRight(String s, int n) {
        return String.format("%-" + n + "s", s);  
    }

    private static String padLeft(String s, int n) {
        return String.format("%" + n + "s", s);  
    }
    
    private static void displayEnding(){
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date dateEnd = new Date();
        
        fileWritter(NEXT_LINE);
        fileWritter("Run Finished: " + dateFormat.format(dateEnd));
        fileWritter(NEXT_LINE);
        fileWritter(NEXT_LINE);
        fileWritter(NEXT_LINE);
    }
    
    private static void fileWritter(String text){
        File file = new File("server.log");
        try (BufferedWriter output = new BufferedWriter(new FileWriter(file, true))) {
            output.append(text);
            System.out.print(text);
            output.close();
        } catch ( IOException e ) {}
    }   
}