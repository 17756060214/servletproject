package com.servlet;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;


public class DataBackupManager {

	private static String VERSION = "1.0";
	
	public static void backup(String[] tables, final OutputStream os) throws IOException {
		
		if(tables == null) throw new IllegalArgumentException("Argument tables cannot be null");
		String[] selects = new String[tables.length];
		
		for(int i=0; i<tables.length; i++){
			int index = tables[i].indexOf(",");
			if(index != -1){
				String spt = tables[i];
				tables[i] = spt.substring(0,index);
				selects[i] = spt.substring(index+1);
			}else{
				selects[i] = "select * from "+tables[i];
			}
		}
		backup(selects, tables, os ,"UTF-8");
	}
	
	public static void backup(String[] selects , String[] tables, final OutputStream os) throws IOException {
		
		if(selects == null ) throw new IllegalArgumentException("Argument selects cannot be null");
		if(tables == null) throw new IllegalArgumentException("Argument tables cannot be null");	
		backup(selects, tables, os , "UTF-8");
	}
		
 
	public static void backup (final String[] selects, final String[] tables, final OutputStream os, final String encoding) throws IOException {
		if(selects == null ) throw new IllegalArgumentException("Argument selects cannot be null");
		if(tables == null) throw new IllegalArgumentException("Argument tables cannot be null");	
		if(selects.length != tables.length){
			throw new IllegalArgumentException("select's length muth equals tables's length");
			
		}
		if(os == null ) throw new IllegalArgumentException("Argument os cannot be null");
		String encode = encoding == null ?"UTF-8":encoding;
		
		final java.io.OutputStreamWriter osw = new java.io.OutputStreamWriter(os, encode);
		final java.io.BufferedWriter bw = new java.io.BufferedWriter(osw);
		try {
			if(tables != null){
				SqlExecutor se = new SqlExecutor();
				
				se.execute(new SqlExecutor.ConnectionCallback() {
					
 					public Object execute(Connection conn) throws SQLException {
 						java.sql.DatabaseMetaData dbmd  = conn.getMetaData();
 						
 						return null;
					}
				})
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
