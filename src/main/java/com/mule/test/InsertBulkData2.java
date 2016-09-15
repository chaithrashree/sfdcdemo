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

public class InsertBulkData2 implements Callable {
    
    @Override
    public Object onCall(MuleEventContext eventContext) throws Exception {
//        InputStream csvInputStream = (InputStream) eventContext.getMessage().getPayload();
    	 LocalMuleClient client = eventContext.getMuleContext().getClient();
    	 Map<String, Object> properties = new HashMap<String, Object>();
    	int noOfRows=0;
        try{

		Connection con=DriverManager.getConnection(
				"jdbc:oracle:thin:@localhost:1521:xe","system","system");
		    if (con != null)
		    {
 			System.out.println("Successfully connected to Oracle DB");
           String fetchQuery="select TABLE_NAME from CONTROL_TABLE where TABLE_NAME='DATA_TABLE'";
          
 			try {
 				PreparedStatement stmt=con.prepareStatement(fetchQuery);  
 		       ResultSet rs= stmt.executeQuery();
 				
 		        while(rs.next())
 		        {
 		        	
 		        	 String tableName=rs.getString(1);
 		           System.out.println(tableName);
 		           fetchQuery="select * from "+tableName;
 		          PreparedStatement stmt1=con.prepareStatement(fetchQuery);  
 	 		       ResultSet rs1= stmt1.executeQuery();
 	 		     while(rs1.next())
 	 		     {
 	 		    	 String record=rs1.getString(1)+","+rs1.getString(2)+","+rs1.getInt(3)+","+rs1.getString(4);
 	 		    	// System.out.println(record);
 		        	try {
 		        		
							client.send("vm://InputQueue",record , properties);
							
						} catch (MuleException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
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
        

