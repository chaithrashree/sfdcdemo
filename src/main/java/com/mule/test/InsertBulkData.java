package com.mule.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.mule.api.MuleEventContext;
import org.mule.api.MuleException;
import org.mule.api.client.LocalMuleClient;
import org.mule.api.lifecycle.Callable;

import oracle.jdbc.OracleTypes;

public class InsertBulkData implements Callable {
    
    @Override
    public Object onCall(MuleEventContext eventContext) throws Exception {
//        InputStream csvInputStream = (InputStream) eventContext.getMessage().getPayload();
    	 LocalMuleClient client = eventContext.getMuleContext().getClient();
    	int noOfRows=0;
        try{

		Connection con=DriverManager.getConnection(
				"jdbc:oracle:thin:@localhost:1521:xe","system","system");
		    if (con != null)
		    {
 			System.out.println("Successfully connected to Oracle DB");
           String fetchQuery="select count(*) from DATA_TABLE";
          
 			try {
 				PreparedStatement stmt=con.prepareStatement(fetchQuery);  
 				
 				
 		       ResultSet rs= stmt.executeQuery();
 				
 		        while(rs.next())
 		        {
 		        	 noOfRows=rs.getInt(1);
 		          System.out.println("total no of rows :"+noOfRows);
 		      
 		        }
 		        int iterate = noOfRows/8000;
 		       int j=1;
 		       int k=8000;
 		      Map<String, Object> properties = new HashMap<String, Object>();
 			  for (int i = 0; i < iterate+1; i++) {
 				  
 				  if(i== iterate)
 				  {
 					 fetchQuery ="SELECT * FROM ( select m.*, rownum r from DATA_TABLE m) WHERE r BETWEEN "+j+" AND "+noOfRows;
 					 
 					PreparedStatement stmt1=con.prepareStatement(fetchQuery);
 					 ResultSet rs1= stmt1.executeQuery();
 		 				
 		 		        while(rs1.next())
 		 		        {
 		 		        	/*int count=rs1.getInt(1);
 		 		          System.out.println(count);*/
 		 		     
 		 		      String record=rs1.getString(1)+","+rs1.getString(2)+","+rs1.getInt(3)+","+rs1.getString(4);
 						
 						try {
 							client.send("vm://InputQueue",record , properties);
 						} catch (MuleException e) {
 							// TODO Auto-generated catch block
 							e.printStackTrace();
 						}
 		 		        }
 		 		        
 		 		     
 				  }
 				  else
 				  {
 				 fetchQuery ="SELECT * FROM ( select m.*, rownum r from DATA_TABLE m) WHERE r BETWEEN "+j+" AND "+k;
 				PreparedStatement stmt2=con.prepareStatement(fetchQuery);
				j=k+1;
				k=k+8000;
				 ResultSet rs1= stmt2.executeQuery();
	 				
	 		        while(rs1.next())
	 		        {
	 		        	 /*int count=rs1.getInt(1);
	 		          System.out.println(count);*/
	 		        	String record=rs1.getString(1)+","+rs1.getString(2)+","+rs1.getInt(3)+","+rs1.getString(4);
						
						try {
							client.send("vm://InputQueue",record , properties);
						} catch (MuleException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	 		        }	
	 		        

					
 				  }
			}
 				
 			   } 
 			 catch (SQLException e)
 			  {
 				 e.printStackTrace();
 			   }
  
 		   } 
		    
		    else 
		    {
 			System.out.println("nFailed to connect to Oracle DB");
 		    }
			con.close();

		 }catch(Exception e){ System.out.println(e);}

				
 
        return "DB processed"; // It can return a different payload of your choice
    }

  

}