import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class part1
{
    public static void main(String args[]) throws SQLException 
    {
        Connection con;
        Statement stmt;
        ResultSet pidRS, mutRS;
        
        /* Oracle Database credentials */
        String url = "jdbc:oracle:thin:@query.engr.uconn.edu:1521:BIBCI";
        String user = "cse4701";
        String password = "datamine";

        Connection my_con;
        Statement my_stmt;
        
        /* MySQL Database credentials */
        String my_url = "jdbc:mysql://localhost:3306/proj2";
        String my_user = "root";
        String my_pw = "Passw0rd";

        String genes[] = {"APC", "TP53", "KRAS", "PIK3CA", "PTEN", "ATM", "MUC4", "SMAD4", "SYNE1", "FBXW7" };
        
        try 
        {
        	// connect to oracle database
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            con = DriverManager.getConnection(url, user, password);
            stmt = con.createStatement();
          
            // connect to mySQL database
            DriverManager.registerDriver(new com.mysql.jdbc.Driver());
            my_con = DriverManager.getConnection(my_url, my_user, my_pw);
            my_stmt = my_con.createStatement();
            
            // check if ig_ready exists, if so delete the table
            DatabaseMetaData dbCheck = my_con.getMetaData();
            ResultSet dbResults = dbCheck.getTables(null, null, "proj2.ig_ready", null);
            if(dbResults.next())
            	my_stmt.executeUpdate("DROP TABLE proj2.ig_ready");

            // create the ig_ready table
            String createTable = "CREATE TABLE `proj2`.`ig_ready` (`patient_id` VARCHAR(25) NOT NULL,"
                												+ "`APC` INT NULL DEFAULT 0,"
                												+ "`TP53` INT NULL DEFAULT 0,"
                												+ "`KRAS` INT NULL DEFAULT 0,"
                												+ "`PIK3CA` INT NULL DEFAULT 0,"
                												+ "`PTEN` INT NULL DEFAULT 0,"
                												+ "`ATM` INT NULL DEFAULT 0,"
                												+ "`MUC4` INT NULL DEFAULT 0,"
                												+ "`SMAD4` INT NULL DEFAULT 0,"
                												+ "`SYNE1` INT NULL DEFAULT 0,"
                												+ "`FBXW7` INT NULL DEFAULT 0,"
                												+ "`status` INT NULL DEFAULT 0,"
                												+ "PRIMARY KEY (`patient_id`))";
            my_stmt.execute(createTable);
   
            // insert all patients from clinical and their status into ig_ready and set all other values with their default value of 0
            String sql = "SELECT DISTINCT patient_id, os_status FROM CLINICAL";
            pidRS = stmt.executeQuery(sql);
            int count = 0;
            while (pidRS.next()) 
	        {
            	String p_id=pidRS.getString(1);																	 // patient_id
            	String status = pidRS.getString(2).equals("LIVING") ? Integer.toString(1): Integer.toString(0) ; // os_status
            	//System.out.println(pidRS.getString(1) + "\t" + pidRS.getString(2));							 // used for testing
            	String mysqlInsertPID = "INSERT INTO proj2.ig_ready (patient_id, status) VALUES ('" + p_id + "', '" + status + "')" ;
            	my_stmt.executeUpdate(mysqlInsertPID);
            	count++;
	        }
            System.out.println("Done inserting. Number of entries inserted: " + count);
                 
            // get all the mutations and update the table by setting the value in the col to 1
            String sql2 = "SELECT patient_id, gene_symbol, variant_classification FROM MUTATION";
            mutRS = stmt.executeQuery(sql2);
            int update = 0;
            while (mutRS.next()) 
	        {
            	for(int i=0; i<genes.length; i++)
            	{
            		
            		String pID = mutRS.getString(1).substring(0,12);											// patient_id
            		String geneSym = mutRS.getString(2);														// gene_symbol
            		String varClass = mutRS.getString(3);														// variation_classification
            		//System.out.println(pID + "\t" + geneSym + "\t" + varClass);								// used for testing
            		String sqlUpdate = "UPDATE proj2.ig_ready SET " + genes[i] + " = 1 WHERE patient_id = '" + pID +"'";
            		if(geneSym.equals(genes[i]) && !varClass.equals("Silent"))									// if a non silent mutation is present update value
            			my_stmt.executeUpdate(sqlUpdate);
            		update++;
            	}
	        }
            System.out.println("Done updating. Number of entries updated: " + update);

            con.close();
            my_con.close();
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(DBTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}