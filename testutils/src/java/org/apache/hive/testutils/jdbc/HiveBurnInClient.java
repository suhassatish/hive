package org.apache.hive.testutils.jdbc;


import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;


public class HiveBurnInClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * Creates 2 tables to query from
     * @param num
     */
    public static void createTables(Connection con) throws SQLException{        
        Statement stmt = con.createStatement();        
        String tableName = "table1";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key int, value string)");

        // load data into table
        // NOTE: filepath has to be local to the hive server  
        String filepath = "./examples/files/kv1.txt";
        String sql = "load data local inpath '" + filepath + "' into table " + tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);

        tableName = "table2";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key int, value string)");

        filepath = "./examples/files/kv2.txt";
        sql = "load data local inpath '" + filepath + "' into table " + tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);
    }
    

    /**
     * 
     * @param con
     * @param numberOfQueryIterations
     * @throws SQLException
     */    
    public static void runQueries(Connection con, int numberOfQueryIterations) throws SQLException {    	
    	Statement stmt = con.createStatement();
    	
    	for (int i=0; i<numberOfQueryIterations; i++){    		
    		System.out.println("Iteration #"+i);
    		// select query
            String sql = "from table1 SELECT * group by table1.key order by table1.key desc";
            System.out.println("Running: " + sql);
            
            long startTime = System.currentTimeMillis();
            ResultSet res = stmt.executeQuery(sql);
            long endTime = System.currentTimeMillis();
            long msElapsedTime = endTime - startTime;
            
            while (res.next()) {
                System.out.println(res.getInt(1));
              }            
            System.out.printf("Time taken for query = %d ms \n", msElapsedTime);
            

            // count query
            sql = "select count(*) from table1" ;
            
            System.out.println("Running: " + sql);
            startTime = System.currentTimeMillis();
            res = stmt.executeQuery(sql);  
            endTime = System.currentTimeMillis();
            msElapsedTime = endTime - startTime;
            
            while (res.next()) {
                System.out.println(res.getInt(1));
              }
            System.out.printf("Time taken for query = %d ms \n", msElapsedTime);
            
            
            // join with group-by, having, order-by  
            sql = "select t1.key,count(t1.key) as cnt from table1 t1"
            		+ " join table2 t2 on (t1.key = t2.key) group by t1.key having cnt > 5 order by cnt desc" ;
            
            System.out.println("Running: " + sql);
            startTime = System.currentTimeMillis();
            res = stmt.executeQuery(sql);  
            endTime = System.currentTimeMillis();
            msElapsedTime = endTime - startTime;
            while (res.next()) {
                System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
              }
            System.out.printf("Time taken for query = %d ms \n", msElapsedTime);
            
            // full outer join
            sql = "select table1.value, table2.value from table1 full outer join table2 "
            		+ "on (table1.key = table2.key)";
            
            
            System.out.println("Running: " + sql);
            startTime = System.currentTimeMillis();
            res = stmt.executeQuery(sql);
            endTime = System.currentTimeMillis();
            msElapsedTime = endTime - startTime;
            
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
              }
            System.out.printf("Time taken for query = %d ms \n", msElapsedTime);
            
    	}
        
    }
    /**
     * @param args
     * @throws SQLException
     * @throws ClassNotFoundException 
     */
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
    	Class.forName(driverName);        
        int numberOfQueryIterations = 10000; //default 10k (runs slightly over 1 day long)

        if (args.length > 0){            
                numberOfQueryIterations = Integer.parseInt(args[0]);                        
        }     
        if (numberOfQueryIterations < 0){
        	numberOfQueryIterations = 10000;
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");        
        createTables(con);
        runQueries(con, numberOfQueryIterations);
    }
}