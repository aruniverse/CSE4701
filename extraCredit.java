import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class extraCredit
{
    public static void main(String args[]) throws SQLException 
    {
        Connection con;
        Statement stmt;
        ResultSet aRS, bRS, cRS, igRS;
        
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

        String genes[] = {"APC", "TP53", "KRAS", "PIK3CA", "PTEN", "ATM", "MUC4", "SMAD4", "SYNE1", "FBXW7"};
        
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
            
            // check if table already exists, if so delete the table
            DatabaseMetaData dbCheck = my_con.getMetaData();
            ResultSet dbResults = dbCheck.getTables(null, null, "proj2.extra_credit", null);
            if(dbResults.next())
            	my_stmt.executeUpdate("DROP TABLE proj2.extra_credit");

            // create the CDT table
            String createTable =  "CREATE TABLE `proj2`.`extra_credit` ("
            									+ "`Gi` VARCHAR(25) NOT NULL, "
            									+ "`Gj` VARCHAR(25) NOT NULL, "
            									+ "`A` INT NULL DEFAULT 0, "
            									+ "`B` INT NULL DEFAULT 0, "
            									+ "`C` INT NULL DEFAULT 0, "
            									+ "`D` INT NULL DEFAULT 0, "
            									+ "`IG` DOUBLE NULL DEFAULT 0, "
            									+ " PRIMARY KEY (`Gi`, `Gj`))";
            
            my_stmt.execute(createTable);
           
            int count = 0;            
            for(int g1=0; g1<genes.length; g1++)
            {
            	for(int g2=g1+1; g2<genes.length; g2++)
            	{
            		String gi = genes[g1];
                	String gj = genes[g2];
                	
                	String sqlA = "SELECT COUNT(*) FROM ig_ready WHERE STATUS='Y' AND " + gi + " = 1 AND " + gj + " = 1";
                	String sqlB = "SELECT COUNT(*) FROM ( SELECT * from ig_ready where Status='Y' MINUS SELECT * from ig_ready where " + gi + " = 1 AND " + gj + " = 1)"; 	
                    String sqlC = "SELECT COUNT(*) FROM ( SELECT * from ig_ready where " + gi + " = 1 AND " + gj + " = 1 MINUS SELECT * from ig_ready where Status='Y' )"; 	
                	
                    double a=0, b=0, c=0, d=627, k=627;
                    
                	aRS = stmt.executeQuery(sqlA);
                	while(aRS.next())
            			a = aRS.getInt(1);
                	
                	bRS = stmt.executeQuery(sqlB);
                	while(bRS.next())
            			b = bRS.getInt(1);
                	
                	cRS = stmt.executeQuery(sqlC);
                	while(cRS.next())
            			c = cRS.getInt(1);
                	
                	d -= (a+b+c);
           	
                	double infoD = -(130/k)*(Math.log(130/k)/Math.log(2)) -(497/k)*(Math.log(497/k)/Math.log(2));	// I(130,497)
                	double I1 = (a==0 || c==0) ? 0 : ((a+c)/k) * (-(a/(a+c))*(Math.log(a/(a+c))/Math.log(2)) -(c/(a+c))*(Math.log(c/(a+c))/Math.log(2))); //I(a,c)
                	double I0 = (b==0 || d==0) ? 0 : ((b+d)/k) * (-(b/(b+d))*(Math.log(b/(b+d))/Math.log(2)) -(d/(b+d))*(Math.log(d/(b+d))/Math.log(2))); //I(b,d)
                	
                	double info_gene_D = I1 + I0;
                	double ig = infoD - info_gene_D;
                	
                	System.out.println("Entry: " + count + "\t" + gi + "-" + gj /*+ "\t a = " + a + "\t b = " + b + "\t c = " + c + "\t d = " + d*/ + "\t ig = " + ig);
                	String mysqlInsertGene = "INSERT INTO proj2.extra_credit (Gi, Gj, A, B, C, D, IG) VALUES ('" + gi + "', '" + gj +"', '" + a + "', '" + b + "', '" + c + "', '" + d + "', '" + ig +  "')" ;
                	my_stmt.executeUpdate(mysqlInsertGene);
                	System.out.println("\tCDT"  + "\t" + a + "\t" + c + "\n\t\t" + b + "\t" + d);
                	System.out.println();
                	count++;
            	}
            }
            System.out.println("Done inserting. Number of entries inserted: " + count);
            
            System.out.println("\nExtra Credit: Data Mining Gene-Pair IG");
            System.out.println("Gi \t Gj \t IG \t\t\t oct");
            String igSQL = "SELECT Gi, Gj, IG, A FROM extra_credit ORDER BY IG DESC";
            igRS = my_stmt.executeQuery(igSQL);
            
            while(igRS.next())
            {
            	String gene1 = igRS.getString(1);
            	String gene2 = igRS.getString(2);
            	String igVal = igRS.getString(3);
            	String ocnt = igRS.getString(4);
            	System.out.println(gene1 + "\t " + gene2 + "\t" + igVal + "\t" + ocnt);
            }   
            
            con.close();
            my_con.close();
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(DBTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}