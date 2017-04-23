import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class part2
{
    public static void main(String args[]) throws SQLException 
    {
        Connection con;
        Statement stmt;
        ResultSet aRS, bRS, cRS, igRS, ocntRS;
        
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
            ResultSet dbResults = dbCheck.getTables(null, null, "proj2.part2", null);
            if(dbResults.next())
            	my_stmt.executeUpdate("DROP TABLE proj2.part2");

            // create the CDT table
            String createTable = "CREATE TABLE `proj2`.`part2` (`Gene` VARCHAR(25) NOT NULL,"
                												+ "`A` INT NULL DEFAULT 0,"
                												+ "`B` INT NULL DEFAULT 0,"
                												+ "`C` INT NULL DEFAULT 0,"
                												+ "`D` INT NULL DEFAULT 0,"
                												+ "`IG` DOUBLE NULL DEFAULT 0,"
                												+ "PRIMARY KEY (`Gene`))";
            my_stmt.execute(createTable);
           
            int count = 0;            
            for(String g : genes)
            {
            	String sqlA = "SELECT COUNT(*) FROM ig_ready WHERE STATUS='Y' AND " + g + " = 1"; 
            	String sqlB = "SELECT COUNT(*) FROM ( SELECT * from ig_ready where Status='Y' MINUS SELECT * from ig_ready where " + g + " = 1 )"; 	
                String sqlC = "SELECT COUNT(*) FROM ( SELECT * from ig_ready where " + g + " = 1 MINUS SELECT * from ig_ready where Status='Y' )"; 	
            	
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
            	
            	System.out.println("Entry " + count + "\t Gene: \t" + g /*+ "\t a = " + a + "\t b = " + b + "\t c = " + c + "\t d = " + d */+ "\t ig = " + ig);
            	String mysqlInsertGene = "INSERT INTO proj2.part2 (Gene, A, B, C, D, IG) VALUES ('" + g + "', '" + a + "', '" + b + "', '" + c + "', '" + d + "', '" + ig +  "')" ;
            	my_stmt.executeUpdate(mysqlInsertGene);
            	System.out.println("CDT \t" + a + "\t" + c + "\n\t" + b + "\t" + d);
            	//System.out.println("INFO(D) = " + infoD + "\t I1(D) = " + I1 + "\t I0 = " + I0 + "\t INFO_g_(D) = " + info_gene_D + "\t IG = " + ig);
            	System.out.println();
            	count++;
            }
            System.out.println("Done inserting. Number of entries inserted: " + count);
            
            System.out.println("\nPart a: Data Mining IG");
            System.out.println("Gene \t IG");
            String igSQL = "SELECT GENE, IG FROM part2 ORDER BY IG DESC";
            igRS = my_stmt.executeQuery(igSQL);
            
            int i=0;
            while(i<5)
            {
            	igRS.next();
            	String gene = igRS.getString(1);
            	String igVal = igRS.getString(2);
            	System.out.println(gene + "\t" + igVal);
            	i++;
            }   
            
            System.out.println("\nPart b: Overlap_CNT");
            System.out.println("Gene \t IG \t\t\t Overlap_CNT");
            String ocntSQL = "SELECT GENE, IG, A FROM part2 ORDER BY IG DESC";
            ocntRS = my_stmt.executeQuery(ocntSQL);
            
            while(i<10)
            {
            	ocntRS.next();
            	String gene = ocntRS.getString(1);
            	String igVal = ocntRS.getString(2);
            	String ocnt = ocntRS.getString(3);
            	System.out.println(gene + "\t" + igVal + "\t" + ocnt);
            	i++;
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