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

import javax.swing.*;
import java.awt.event.*;
import java.awt.*;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author Yong Sen & Jensen
 */
public class SqlComparator {

    /**
     * @param args the command line arguments
     */
    private static String MAIN_DB_SERVER;
    private static String MAIN_DATABASE_NAME;
    private static String MAIN_DATABASE_SCHEMA;
    private static String CONNECTION_STRING_MAIN;
    private static String USER_MAIN;
    private static String PWD_MAIN;
    
    private static String SECOND_DB_SERVER;
    private static String SECOND_DATABASE_NAME;
    private static String SECOND_DATABASE_SCHEMA;
    private static String CONNECTION_STRING_SECOND;
    private static String USER_SECOND;
    private static String PWD_SECOND;
    
    private static String NEXT_LINE = System.lineSeparator();
    private static String TAB = "  ";
    private static String BREAK_LINE = "=========================================================================================================================";
    
    private static String MARIA = "MARIA";
    private static String MSSQL = "MSSQL";
    private static String PGSQL = "PGSQL";
    
    public SqlComparator(){
    }
    
    public static void main(String[] args) throws SQLException {

//        if(!setParams(args)){
//            
//            fileWritter("Parameter missing, please check the batch file");
//            
//            return;
//        }
//        
//        if(!checkDBServerAvailability()){
//            fileWritter("Database server to compare are not supported currently. Only allow MARIADB, MSSQL, PGSQL");
//        }
        
        showFrame();
    }
    
    private static void startCompare() throws SQLException{
        Map<String, Integer> countOfTableResultMap = getCountOfTable();
        Map<String, String> tableNameResultMap = getTableName();
        
        StringBuilder output = new StringBuilder();
        
        boolean compareNumberOfTable = countOfTableResultMap.get("Main_Num_Of_Table").equals(countOfTableResultMap.get("Second_Num_Of_Table"));
        boolean compareTableName = tableNameResultMap.get("Main_Table_Name").equals(tableNameResultMap.get("Second_Table_Name"));
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date dateStart = new Date();

        output.append("Run SqlComparator: " + dateFormat.format(dateStart));
        output.append(NEXT_LINE);
        output.append("Main DB Server: " + MAIN_DB_SERVER);
        output.append(NEXT_LINE);
        output.append("Second DB Server: " + SECOND_DB_SERVER);
        output.append(NEXT_LINE);
        output.append(NEXT_LINE);
        output.append("Database Migration Validation");
        output.append(NEXT_LINE);
        output.append(BREAK_LINE);
        output.append(NEXT_LINE);
        output.append(padRight("No.", 5));
        output.append(padRight("Number Of Table (Main)", 30));
        output.append(padRight("Number Of Table (Second)", 30));
        output.append(NEXT_LINE);
        output.append(padRight("1", 5));
        output.append(padRight(countOfTableResultMap.get("Main_Num_Of_Table")+"", 30));
        output.append(padRight(countOfTableResultMap.get("Second_Num_Of_Table")+"", 30));
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
    
    private static void showFrame(){
        JFrame f = new JFrame("SQL Comparator");
        JPanel panelMain = new JPanel();
        JPanel panelSecond = new JPanel();
        
        ButtonGroup bgMain = new ButtonGroup();  
        ButtonGroup bgSecond = new ButtonGroup();  
        
        final JRadioButton rbMainMSSQL = new JRadioButton("MSSQL", true);
        final JRadioButton rbMainPGSQL = new JRadioButton("PostgreSQL");
        final JRadioButton rbMainMARIA = new JRadioButton("MariaDB");
        
        final JRadioButton rbSecondMSSQL = new JRadioButton("MSSQL", true);
        final JRadioButton rbSecondPGSQL = new JRadioButton("PostgreSQL");
        final JRadioButton rbSecondMARIA = new JRadioButton("MariaDB");
        
        bgMain.add(rbMainMSSQL);
        bgMain.add(rbMainPGSQL);
        bgMain.add(rbMainMARIA);
        
        bgSecond.add(rbSecondMSSQL);
        bgSecond.add(rbSecondPGSQL);
        bgSecond.add(rbSecondMARIA);
        
        
        final JTextField tfMainDBServer = new JTextField("MSSQL");
        final JTextField tfMainDomainName = new JTextField("localhost:1433");
        final JTextField tfMainDatabase = new JTextField("RBGCLIENT");
        final JTextField tfMainSchema = new JTextField("dbo");
        final JTextField tfMainUser = new JTextField("rbguser");
        final JTextField tfMainPwd = new JTextField("rbgu53r");
        
        final JTextField tfSecondDBServer = new JTextField("MARIA");
        final JTextField tfSecondDomainName = new JTextField("localhost:3306");
        final JTextField tfSecondDatabase = new JTextField("RBGCLIENT1");
        final JTextField tfSecondSchema = new JTextField("RBGCLEINT");
        final JTextField tfSecondUser = new JTextField("rbguser");
        final JTextField tfSecondPwd = new JTextField("rbgu53r");
        
        final JLabel lMainDBServer = new JLabel("Main Database");
        final JLabel lMainDBType = new JLabel("MSSQL/PGSQL/MARIA:");
        final JLabel lMainDomainName = new JLabel("Server Name:");
        final JLabel lMainDatabaseName = new JLabel("Database Name:");
        final JLabel lMainSchema = new JLabel("Schema:");
        final JLabel lMainUser = new JLabel("User:");
        final JLabel lMainPwd = new JLabel("Password:");
        
        final JLabel lSecondDBServer = new JLabel("Second Database");
        final JLabel lSecondDBType = new JLabel("MSSQL/PGSQL/MARIA:");
        final JLabel lSecondDomainName = new JLabel("Server Name:");
        final JLabel lSecondDatabaseName = new JLabel("Database Name:");
        final JLabel lSecondSchema = new JLabel("Schema:");
        final JLabel lSecondUser = new JLabel("User:");
        final JLabel lSecondPwd = new JLabel("Password:");
        
        panelMain.setBounds(20, 40, 470, 290);
        panelSecond.setBounds(510, 40, 470, 290);
        
        panelMain.setBorder(BorderFactory.createLineBorder(Color.black));
        panelSecond.setBorder(BorderFactory.createLineBorder(Color.black));
        
        lMainDBServer.setBounds(20, 20, 470, 20);
        lSecondDBServer.setBounds(510, 20, 470, 20);
        
        lMainDBType.setBounds(30, 45, 450, 20);
//        tfMainDBServer.setBounds(30, 65, 450, 20);

        rbMainMSSQL.setBounds(30, 65, 140, 20);
        rbMainPGSQL.setBounds(180, 65, 140, 20);
        rbMainMARIA.setBounds(330, 65, 140, 20);
        
        lMainDomainName.setBounds(30, 90, 450, 20);
        tfMainDomainName.setBounds(30, 110, 450, 20);
        
        lMainDatabaseName.setBounds(30, 135, 450, 20);
        tfMainDatabase.setBounds(30, 155, 450, 20);
        
        lMainSchema.setBounds(30, 180, 450, 20); 
        tfMainSchema.setBounds(30, 200, 450, 20);
        
        lMainUser.setBounds(30, 225, 450, 20);
        tfMainUser.setBounds(30, 245, 450, 20);
        
        lMainPwd.setBounds(30, 270, 450, 20);
        tfMainPwd.setBounds(30, 290, 450, 20);
        
        f.add(lMainDBType);
//        f.add(tfMainDBServer);
        f.add(rbMainMSSQL);
        f.add(rbMainPGSQL);
        f.add(rbMainMARIA);
        f.add(lMainDomainName);
        f.add(tfMainDomainName);
        f.add(lMainDatabaseName);
        f.add(tfMainDatabase);
        f.add(lMainSchema);
        f.add(tfMainSchema);
        f.add(lMainUser);
        f.add(tfMainUser);
        f.add(lMainPwd);
        f.add(tfMainPwd);
        
        lSecondDBType.setBounds(520, 45, 450, 20);
        //tfSecondDBServer.setBounds(520, 65, 450, 20);
        rbSecondMSSQL.setBounds(520, 65, 140, 20);
        rbSecondPGSQL.setBounds(670, 65, 140, 20);
        rbSecondMARIA.setBounds(820, 65, 140, 20);
        
        lSecondDomainName.setBounds(520, 90, 450, 20);
        tfSecondDomainName.setBounds(520, 110, 450, 20);
        
        lSecondDatabaseName.setBounds(520, 135, 450, 20);
        tfSecondDatabase.setBounds(520, 155, 450, 20);
        
        lSecondSchema.setBounds(520, 180, 450, 20); 
        tfSecondSchema.setBounds(520, 200, 450, 20);
        
        lSecondUser.setBounds(520, 225, 450, 20);
        tfSecondUser.setBounds(520, 245, 450, 20);
        
        lSecondPwd.setBounds(520, 270, 450, 20);
        tfSecondPwd.setBounds(520, 290, 450, 20);
        
        f.add(lSecondDBType);
        //f.add(tfSecondDBServer);
        f.add(rbSecondMSSQL);
        f.add(rbSecondPGSQL);
        f.add(rbSecondMARIA);
        f.add(lSecondDomainName);
        f.add(tfSecondDomainName);
        f.add(lSecondDatabaseName);
        f.add(tfSecondDatabase);
        f.add(lSecondSchema);
        f.add(tfSecondSchema);
        f.add(lSecondUser);
        f.add(tfSecondUser);
        f.add(lSecondPwd);
        f.add(tfSecondPwd);
        
        JButton b = new JButton("Start Compare");  
        b.setBounds(390,360,200,30);  
        b.addActionListener(new ActionListener(){  
            public void actionPerformed(ActionEvent e){
                
                if(rbMainMSSQL.isSelected()){
                    MAIN_DB_SERVER = "MSSQL";
                    CONNECTION_STRING_MAIN = "jdbc:sqlserver://" + tfMainDomainName.getText() + ";databaseName=" + tfMainDatabase.getText() + ";trustServerCertificate=true;";
                }
                else if(rbMainPGSQL.isSelected()){
                    MAIN_DB_SERVER = "PGSQL";
                    CONNECTION_STRING_MAIN = "jdbc:postgresql://" + tfMainDomainName.getText() + "/" + tfMainDatabase.getText();
                }
                else{
                    MAIN_DB_SERVER = "MARIA";
                    CONNECTION_STRING_MAIN = "jdbc:mariadb://" + tfMainDomainName.getText() + "/" + tfMainDatabase.getText();
                }
                MAIN_DATABASE_NAME = tfMainDatabase.getText();
                MAIN_DATABASE_SCHEMA = tfMainSchema.getText();
                USER_MAIN = tfMainUser.getText();
                PWD_MAIN = tfMainPwd.getText();
                
                if(rbSecondMSSQL.isSelected()){
                    SECOND_DB_SERVER = "MSSQL";
                    CONNECTION_STRING_SECOND = "jdbc:sqlserver://" + tfSecondDomainName.getText() + ";databaseName=" + tfSecondDatabase.getText() + ";trustServerCertificate=true;";
                }
                else if(rbSecondPGSQL.isSelected()){
                    SECOND_DB_SERVER = "PGSQL";
                    CONNECTION_STRING_SECOND = "jdbc:postgresql://" + tfSecondDomainName.getText() + "/" + tfSecondDatabase.getText();
                }
                else{
                    SECOND_DB_SERVER = "MARIA";
                    CONNECTION_STRING_SECOND = "jdbc:mariadb://" + tfSecondDomainName.getText() + "/" + tfSecondDatabase.getText();
                }
                SECOND_DATABASE_NAME = tfSecondDatabase.getText();
                SECOND_DATABASE_SCHEMA = tfSecondSchema.getText();
                USER_SECOND = tfSecondUser.getText();
                PWD_SECOND = tfSecondPwd.getText();
                
                try {
                    f.setVisible(false);
                    startCompare();
                    
                } catch (SQLException ex) {
                    Logger.getLogger(SqlComparator.class.getName()).log(Level.SEVERE, null, ex);
                }
            }  
        });    
        
        f.add(lMainDBServer);
        f.add(lSecondDBServer);
        f.add(panelMain);
        f.add(panelSecond);
        
        f.add(b);

        f.setSize(1020,500);  
        f.setLayout(null);  
        f.setVisible(true);   
    }    
    
    
    private static boolean checkDBServerAvailability(){
        return 
            ((MAIN_DB_SERVER.equalsIgnoreCase(MSSQL) || MAIN_DB_SERVER.equalsIgnoreCase(MARIA) || MAIN_DB_SERVER.equalsIgnoreCase(PGSQL))
            &&
            (SECOND_DB_SERVER.equalsIgnoreCase(MSSQL) || SECOND_DB_SERVER.equalsIgnoreCase(MARIA) || SECOND_DB_SERVER.equalsIgnoreCase(PGSQL)));
    }

    private static boolean setParams(String[] args){
        boolean status = false;
        if(args.length == 8){
            MAIN_DB_SERVER = args[0];
            CONNECTION_STRING_MAIN = args[1];
            USER_MAIN = args[2];
            PWD_MAIN = args[3];
            SECOND_DB_SERVER = args[4];
            CONNECTION_STRING_SECOND = args[5];
            USER_SECOND = args[6];
            PWD_SECOND = args[7];
            
            status = true;
        }
        return status;
    }
    
    private static Map<String, Integer> getCountOfTable() throws SQLException{
        
        String countTableQueryMSSQLAndPGSQL = "SELECT COUNT(*) AS total FROM INFORMATION_SCHEMA.tables WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'";
        String countTableQueryMARIA = countTableQueryMSSQLAndPGSQL.replace("dbo", "rbgclient");
        
        String queryMain = "";
        String querySecond = "";
        
        if(MAIN_DB_SERVER.equalsIgnoreCase(MARIA)){
            queryMain = countTableQueryMARIA;
        }
        else{
            queryMain = countTableQueryMSSQLAndPGSQL;
        }
        
        if(SECOND_DB_SERVER.equalsIgnoreCase(MARIA)){
            querySecond = countTableQueryMARIA;
        }
        else{
            querySecond = countTableQueryMSSQLAndPGSQL;
        }

        Map<String, Integer> resultMap = new HashMap<>();
        
        ResultSet setMain = queryData(queryMain, MAIN_DB_SERVER);
        ResultSet setSecond = queryData(querySecond, SECOND_DB_SERVER);
        
        if(setMain.next()){
            resultMap.put("Main_Num_Of_Table", setMain.getInt("total"));
        }
        
        if(setSecond.next()){
            resultMap.put("Second_Num_Of_Table", setSecond.getInt("total"));
        }
        
        return resultMap;
    }
    
    private static Map<String, String> getTableName() throws SQLException{
        
        //Remove undercore since different SQL might have different mechanism of sorting
        String tableQueryMSSQLAndPGSQL = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.tables WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo' ORDER BY REPLACE(TABLE_NAME, '_', '') ASC";
        String tableQueryMARIA = tableQueryMSSQLAndPGSQL.replace("dbo", "rbgclient");
        
        String queryMain = "";
        String querySecond = "";
        
        if(MAIN_DB_SERVER.equalsIgnoreCase(MARIA)){
            queryMain = tableQueryMARIA;
        }
        else{
            queryMain = tableQueryMSSQLAndPGSQL;
        }
        
        if(SECOND_DB_SERVER.equalsIgnoreCase(MARIA)){
            querySecond = tableQueryMARIA;
        }
        else{
            querySecond = tableQueryMSSQLAndPGSQL;
        }
        
        Map<String, String> resultMap = new HashMap<>();
        
        ResultSet setMain = queryData(queryMain, MAIN_DB_SERVER);
        ResultSet setSecond = queryData(querySecond, SECOND_DB_SERVER);
        
        //Get ALL TABLE NAME
        String strTableMain = "";
        while(setMain.next()){
            strTableMain += setMain.getString("table_name").toUpperCase() + ",";
        }
        resultMap.put("Main_Table_Name", strTableMain);
        
        //Get ALL TABLE NAME
        String strTableSecond = "";
        while(setSecond.next()){
            strTableSecond += setSecond.getString("table_name").toUpperCase() + ",";
        }
        resultMap.put("Second_Table_Name", strTableSecond);
        
        return resultMap;
    }
    
    private static ArrayList<String> getAllTableName() throws SQLException{
        ArrayList<String> tableArray = new ArrayList<String>();
        
        String tableQueryMSSQLAndPGSQL = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.tables WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo' ORDER BY REPLACE(TABLE_NAME, '_', '') ASC";
        String tableQueryMARIA = tableQueryMSSQLAndPGSQL.replace("dbo", "rbgclient");
        
        String queryMain = "";
        
        if(MAIN_DB_SERVER.equalsIgnoreCase(MARIA)){
            queryMain = tableQueryMARIA;
        }
        else{
            queryMain = tableQueryMSSQLAndPGSQL;
        }
        
        ResultSet setMain = queryData(queryMain, MAIN_DB_SERVER);
        
        while(setMain.next()){
            tableArray.add(setMain.getString("table_name").toUpperCase());
        }
        
        return tableArray;
    }
    
    private static boolean compareTableRow() throws SQLException{
        String printResult = 
            NEXT_LINE + NEXT_LINE +
            "Data Count Validation" + NEXT_LINE +
            BREAK_LINE + NEXT_LINE + 
            padRight("No.", 5) +
            padRight("TABLE_NAME", 50) + 
            padRight("Main DB", 10) + 
            padRight("Second DB", 10) + 
            padRight("Data Count Comparison", 30) + 
            //padRight("Row Name Comparison", 30) + 
            NEXT_LINE + BREAK_LINE + NEXT_LINE;
        
        ArrayList<String> tableArray = getAllTableName();
        
        boolean status = false;
        
        String mssql_pgsql_query = "SELECT * FROM(";
        String maria_query = "SELECT * FROM(";
        
        for(int i=0; i<tableArray.size(); i++){
            mssql_pgsql_query += "SELECT '" + tableArray.get(i) + "' AS table_name, COUNT(*) as row_count FROM \"" + tableArray.get(i) + "\" UNION ALL ";
            maria_query += "SELECT '" + tableArray.get(i) + "' AS table_name, COUNT(*) as row_count FROM " + tableArray.get(i) + " UNION ALL ";
        }
        if(tableArray.size() > 0){
            mssql_pgsql_query = mssql_pgsql_query.substring(0, mssql_pgsql_query.length()-10);
            maria_query = maria_query.substring(0, maria_query.length()-10);
        }
        
        mssql_pgsql_query += ") FINAL ORDER BY REPLACE(TABLE_NAME, '_', '') ASC";
        maria_query += ") FINAL ORDER BY REPLACE(TABLE_NAME, '_', '') ASC";
        
        String queryMain = "";
        String querySecond = "";
        
        if(MAIN_DB_SERVER.equalsIgnoreCase(MARIA)){
            queryMain = maria_query;
        }
        else{
            queryMain = mssql_pgsql_query;
        }
        
        if(SECOND_DB_SERVER.equalsIgnoreCase(MARIA)){
            querySecond = maria_query;
        }
        else{
            querySecond = mssql_pgsql_query;
        }
        
        Map<String, String> resultMap = new HashMap<>();
        
        ResultSet setMain = queryData(queryMain, MAIN_DB_SERVER);
        ResultSet setSecond = queryData(querySecond, SECOND_DB_SERVER);
        
        int i = 1;
        while(setMain.next() && setSecond.next()){

            if(setMain.getString("table_name").equalsIgnoreCase(setSecond.getString("table_name"))){
                printResult += padRight( i + "", 5) +
                                padRight(setMain.getString("table_name"), 50) + 
                                padRight(setMain.getString("row_count"), 10) + 
                                padRight(setSecond.getString("row_count"), 10) + 
                                padRight(setMain.getString("row_count").equalsIgnoreCase(setSecond.getString("row_count"))+"", 30) + 
                                //padRight(sqlResultSet.getString("column_str").equals(mariaResultSet.getString("column_str"))+"", 30) +
                                NEXT_LINE;

                if(setMain.getString("row_count").equalsIgnoreCase(setSecond.getString("row_count"))){
                    status = true;
                }
                else{
                    status = false;
                }

                i++;
            }
        }

        fileWritter(printResult);

        return status;
    }
    
    private static boolean compareTableColumn() throws SQLException{
        
        String printResult = "Column Count and Migration Validation" + NEXT_LINE +
            BREAK_LINE + NEXT_LINE + 
            padRight("No.", 5) +
            padRight("TABLE_NAME", 50) + 
            padRight("Main DB", 10) + 
            padRight("Second DB", 10) + 
            padRight("Count Comparison", 20) + 
            padRight("Column Name Comparison", 30) + 
            NEXT_LINE + BREAK_LINE + NEXT_LINE;
        
        boolean status = false;

        String queryMSSQL = "SELECT C.TABLE_NAME as table_name, COUNT(C.COLUMN_NAME) as column_count, STRING_AGG(C.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY REPLACE(C.COLUMN_NAME,'_','') ASC) as column_str " +
                        "FROM INFORMATION_SCHEMA.columns C, INFORMATION_SCHEMA.tables T " +
                        "WHERE C.TABLE_NAME = T.TABLE_NAME AND C.TABLE_SCHEMA = 'dbo' AND T.TABLE_TYPE = 'BASE TABLE' " +
                        "GROUP BY C.TABLE_NAME " +
                        "ORDER BY REPLACE(C.TABLE_NAME, '_', '') ASC";
        
        String queryMARIA = queryMSSQL.replace("C.TABLE_SCHEMA = 'dbo'", "C.TABLE_SCHEMA = T.TABLE_SCHEMA AND C.TABLE_SCHEMA = 'RBGCLIENT'").replace("STRING_AGG(C.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY REPLACE(C.COLUMN_NAME,'_','') ASC)", "GROUP_CONCAT(C.COLUMN_NAME order by REPLACE(C.COLUMN_NAME,'_','') ASC)");
        
        String queryPGSQL = "SELECT C.TABLE_NAME as table_name, COUNT(C.COLUMN_NAME) as column_count, STRING_AGG(C.COLUMN_NAME, ',' ORDER BY REPLACE(C.COLUMN_NAME,'_','') ASC) as column_str " +
                        "FROM INFORMATION_SCHEMA.columns C, INFORMATION_SCHEMA.tables T " +
                        "WHERE C.TABLE_NAME = T.TABLE_NAME AND C.TABLE_SCHEMA = 'dbo' AND T.TABLE_TYPE = 'BASE TABLE' " +
                        "GROUP BY C.TABLE_NAME " +
                        "ORDER BY REPLACE(C.TABLE_NAME, '_', '') ASC";
        
        String queryMain = "";
        String querySecond = "";
        
        if(MAIN_DB_SERVER.equalsIgnoreCase(MARIA)){
            queryMain = queryMARIA;
        }
        else if(MAIN_DB_SERVER.equalsIgnoreCase(PGSQL)){
            queryMain = queryPGSQL;
        }
        else{
            queryMain = queryMSSQL;
        }
        
        if(SECOND_DB_SERVER.equalsIgnoreCase(MARIA)){
            querySecond = queryMARIA;
        }
        else if(SECOND_DB_SERVER.equalsIgnoreCase(PGSQL)){
            querySecond = queryPGSQL;
        }
        else{
            querySecond = queryMSSQL;
        }
        
        ResultSet setMain = queryData(queryMain, MAIN_DB_SERVER);
        ResultSet setSecond = queryData(querySecond, SECOND_DB_SERVER);
        
        int i = 1;
                
        while(setMain.next() && setSecond.next()){
            if(setMain.getString("table_name").equalsIgnoreCase(setSecond.getString("table_name"))){
                printResult += padRight( i + "", 5) +
                                padRight(setMain.getString("table_name"), 50) + 
                                padRight(setMain.getString("column_count"), 10) + 
                                padRight(setSecond.getString("column_count"), 10) + 
                                padRight(setMain.getString("column_count").equalsIgnoreCase(setSecond.getString("column_count"))+"", 20) + 
                                padRight(setMain.getString("column_str").equalsIgnoreCase(setSecond.getString("column_str"))+"", 30) +
                                NEXT_LINE;

                i++;

                if(
                    setMain.getString("column_count").equalsIgnoreCase(setSecond.getString("column_count"))    
                    &&
                    setMain.getString("column_str").equalsIgnoreCase(setSecond.getString("column_str"))
                ){
                    status = true;
                }
                else{
                    status = false;
                }
            }
        }

        fileWritter(printResult);

        return status;
    }
    
    private static ArrayList<String> getTablePK(String table) throws SQLException{
        
        ArrayList<String> pkArray = new ArrayList<String>();

        String queryMARIA = "select K.COLUMN_NAME as column_name, K.TABLE_NAME as table_name " +
                        "from INFORMATION_SCHEMA.TABLE_CONSTRAINTS T, INFORMATION_SCHEMA.KEY_COLUMN_USAGE K " +
                        "where " +
                        "T.TABLE_NAME = K.TABLE_NAME " +
                        "and T.constraint_schema = K.constraint_schema " +
                        "and T.constraint_schema = 'rbgclient' " +
                        "and T.TABLE_NAME = '" + table.toUpperCase() + "' " +
                        "AND T.CONSTRAINT_TYPE = 'PRIMARY KEY'";
        
        String queryMSSQLAndPGSQL = "SELECT K.COLUMN_NAME as column_name, K.TABLE_NAME as table_name FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE K " +
                "ON K.CONSTRAINT_NAME = T.CONSTRAINT_NAME " +
                "AND K.TABLE_SCHEMA = 'dbo' " +
                "AND T.CONSTRAINT_TYPE='PRIMARY KEY' " +
                "AND K.TABLE_CATALOG = 'RBGCLIENT' " +
                //"AND K.ORDINAL_POSITION = 1 " +
                "AND K.TABLE_NAME = '" + table.toUpperCase() + "'";
        
        String queryMain = "";
        
        if(MAIN_DB_SERVER.equalsIgnoreCase(MARIA)){
            queryMain = queryMARIA;
        }
        else{
            queryMain = queryMSSQLAndPGSQL;
        }
        
        ResultSet setMain = queryData(queryMain, MAIN_DB_SERVER);
        
        while(setMain.next()){
            pkArray.add(setMain.getString("column_name"));
        }
        
        return pkArray;
    }
    
    private static void compareTableData() throws SQLException{
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
            
            ArrayList<String> pkArray = getTablePK(tableArray.get(i));
            String pkMSSQL = "";
            String pkMARIA = "";
            String pkPGSQL = "";
            
            for(int j=0; j<pkArray.size(); j++){
                pkMARIA += "replace(" + pkArray.get(j) + ",'_',''), ";
                pkMSSQL += "replace(\"" + pkArray.get(j) + "\",'_',''), ";
                pkPGSQL += "replace(CAST(\"" + pkArray.get(j) + "\" AS VARCHAR),'_',''), ";  
            }
            
            pkMSSQL = pkMSSQL.substring(0, pkMSSQL.length()-2);
            pkMARIA = pkMARIA.substring(0, pkMARIA.length()-2);
            pkPGSQL = pkPGSQL.substring(0, pkPGSQL.length()-2);

            String queryMSSQL = "SELECT * FROM \"" + tableArray.get(i) + "\" ORDER BY " + pkMSSQL.toUpperCase() + " ASC";
            String queryPGSQL = "SELECT * FROM \"" + tableArray.get(i) + "\" ORDER BY " + pkPGSQL.toUpperCase() + " ASC";
            String queryMARIA = "SELECT * FROM " + tableArray.get(i) + " ORDER BY " + pkMARIA.toUpperCase() + " ASC";
            
            String queryMain = "";
            String querySecond = "";

            if(MAIN_DB_SERVER.equalsIgnoreCase(MARIA)){
                queryMain = queryMARIA;
            }
            else if(MAIN_DB_SERVER.equalsIgnoreCase(PGSQL)){
                queryMain = queryPGSQL;
            }
            else{
                queryMain = queryMSSQL;
            }

            if(SECOND_DB_SERVER.equalsIgnoreCase(MARIA)){
                querySecond = queryMARIA;
            }
            else if(SECOND_DB_SERVER.equalsIgnoreCase(PGSQL)){
                querySecond = queryPGSQL;
            }
            else{
                querySecond = queryMSSQL;
            }
            
            ResultSet mainResultSet = queryData(queryMain, MAIN_DB_SERVER);
            ResultSet secondResultSet = queryData(querySecond, SECOND_DB_SERVER);
            
            //Get Column Count
            ResultSetMetaData mainResultSetMetaData = mainResultSet.getMetaData();
            ResultSetMetaData secondResultSetMetaData = secondResultSet.getMetaData();
            int mainCount = mainResultSetMetaData.getColumnCount();
            int secondCount = secondResultSetMetaData.getColumnCount();
            
            int row = 1;
            while(mainResultSet.next() && secondResultSet.next()){

                if(mainCount == secondCount){

                    for(int j = 1; j <= mainCount && !unmatch; j++){

                        String first = mainResultSet.getString(j);
                        String second = secondResultSet.getString(j);

                        boolean compareResult = false;

                        if(first != null && second != null){
                            //compareResult = first.toString().replaceAll("\\s+","").equalsIgnoreCase(second.toString().replaceAll("\\s+",""));
                            if(mainResultSetMetaData.getColumnTypeName(j) == "xml" && 
                                    secondResultSetMetaData.getColumnTypeName(j) == "xml" && 
                                    (MAIN_DB_SERVER.equalsIgnoreCase(PGSQL)||SECOND_DB_SERVER.equalsIgnoreCase(PGSQL)))
                            {
                                if(MAIN_DB_SERVER.equalsIgnoreCase(PGSQL) && first.length()>0){
                                    first = first.substring(1);
                                }
                                
                                if(SECOND_DB_SERVER.equalsIgnoreCase(PGSQL) && second.length()>0){
                                    second = second.substring(1);
                                }
                            }
                            
                            //compareResult = first.toString().equals(second.toString());
                            compareResult = first.toString().replaceAll("\\s+","").equalsIgnoreCase(second.toString().replaceAll("\\s+",""));
                        }
                        else{
                            compareResult = first == second;
                        }

                        if(!compareResult){
                            unmatch = true;
                            reason = "Data unmatch in row " + row + " at column \"" + mainResultSetMetaData.getColumnName(j) + "\"";
                        }
                    }
                }
                else{
                    reason = "Not able to compare data since column is column unmatch";
                }
                row++;
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
    
    private static ResultSet queryData(String query, String db) throws SQLException{        
        if(db.equals(MAIN_DB_SERVER)){
            Connection sqlConnection = DriverManager.getConnection(CONNECTION_STRING_MAIN, USER_MAIN, PWD_MAIN);
             // retrieve data from MS SQL Server database
            Statement sqlStatement = sqlConnection.createStatement();
            ResultSet sqlResultSet = sqlStatement.executeQuery(query);

            return sqlResultSet;
        }
        else if(db.equals(SECOND_DB_SERVER)){
            Connection sqlConnection = DriverManager.getConnection(CONNECTION_STRING_SECOND, USER_SECOND, PWD_SECOND);
             // retrieve data from MS SQL Server database
            Statement sqlStatement = sqlConnection.createStatement();
            ResultSet sqlResultSet = sqlStatement.executeQuery(query);

            return sqlResultSet;
        }
        else{
            return null;
        }
    } 
}
