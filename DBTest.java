/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
//package edu.uconn.engr.bibci.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * This class is a simple example to access oracle database and list all the objects.
 * We can use a oracle client to browse the database with the credentials mentioned below in the code.
 * The database has to be accessible first. If its not publicly accessible we should use UCONN's VPN to acquire UCONN IP.
 * 
 * To run this file, we have to import ojdbc6.jar file into class library. 
 * If you are using NetBeans IDE, you can right-click on library folder and "Add JAR/Folder" to import the jar file.
 * After you import it, it is going to show in library section of your project.
 * 
 * 
 * SQL Developer is one of the best available oracle GUI clients available.
 * This can be downloaded for free from oracle's website.
 * http://www.oracle.com/technetwork/developer-tools/sql-developer/overview/index.html
 * 
 */

public class DBTest 
{

    public static void main(String args[]) 
    {
        Connection con;
        Statement stmt;
        ResultSet rs;
        ResultSetMetaData rsmd;
        
        /* Database credentials */
        String user = "cse4701";
        String password = "datamine";
        String host = "query.engr.uconn.edu";
        String port = "1521";
        String sid = "BIBCI";
        String url = "jdbc:oracle:thin:@" + host + ":" + port + ":" + sid;

        try 
        {
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            con = DriverManager.getConnection(url, user, password);
            stmt = con.createStatement();

            String sql = "select * from tab";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();

            int count = 0;
            while (rs.next()) 
	        {
	        	ArrayList<Object> obArray = new ArrayList<Object>();
	            for (int i = 0; i < rsmd.getColumnCount(); i++) 
	            {
	            	obArray.add(rs.getObject(i + 1));
	                System.out.println(obArray.toArray()[0]);
	                count++;
	            }
	        }
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(DBTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
