package com.servlet;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Properties;

import org.springframework.jca.cci.core.ConnectionCallback;

public class SqlExecutor {

	
	static Properties prop  =null;
	
	static{
		prop = new Properties();
		try {
			prop.load(SQLException.class.getClassLoader().getResourceAsStream("db.properties"));
			Class.forName(prop.getProperty("jdbc.driver"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private Connection conn;
	private boolean readOnly;
	private boolean autoCommit;
	private String catalog;
	private int transactionIsolation = 2;
	public static Properties getProp() {
		return prop;
	}
	public static void setProp(Properties prop) {
		SqlExecutor.prop = prop;
	}
	public Connection getConn() {
		return conn;
	}
	public void setConn(Connection conn) {
		this.conn = conn;
	}
	public boolean isReadOnly() {
		return readOnly;
	}
	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}
	public boolean isAutoCommit() {
		return autoCommit;
	}
	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}
	public String getCatalog() {
		return catalog;
	}
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}
	public int getTransactionIsolation() {
		return transactionIsolation;
	}
	public void setTransactionIsolation(int transactionIsolation) {
		this.transactionIsolation = transactionIsolation;
	}
	
	
	public boolean execute(final String sql, final Object... params) {
		
		return (Boolean)doExecute(sql,new PreparedStatementCallback(){
			public Object execute(Connection conn, PreparedStatement stmt)
			throws SQLException {
				setParameters(stmt, params);
				return stmt.execute();
			}
		});
		
	}
	
	public Object execute(String sql, PreparedStatementCallback callback){
		return doExecute(sql,callback);
	}
	
	public Object execute(ConnectionCallback callback) {
		return doExecute(callback);
		
	}
	
	private Object doExecute(ConnectionCallback callback){
		Object result = null;
		try {
			conn =  getConnection();
			
			beforeExecute(conn);
			
			result = callback.execute(conn);
			
			conn.commit();
		} catch (SQLException e) {
			 try {
				conn.rollback();
			} catch (Exception e2) {
				System.out.println(e2.getMessage());
			}
			 throw new RuntimeException();
		}finally{
			try {
				if(conn != null ){
					conn.close();
				}
			} catch (Exception e ) {
				System.out.println(e.getMessage());
			}
		}
		return result;
	}
	
	 private Object doExecute(String sql, PreparedStatementCallback callback) {
	        if(sql == null) return null;
	        PreparedStatement stmt = null;
	        Object result = null;
	        try {
	            conn = getConnection();
	             
	            beforeExecute(conn);
	             
	            stmt = conn.prepareStatement(sql);
	            result = callback.execute(conn, stmt);
	             
	            conn.commit();
	        } catch (SQLException e) {
	            try {
	                conn.rollback();
	            } catch (SQLException e1) {
	                System.out.println(e1.getMessage());
	            }
	            throw new RuntimeException(e);
	        }finally{
	            try {
	                if(stmt != null)stmt.close();
	                if(conn != null)conn.close();
	            } catch (SQLException e) {
	                System.out.println(e.getMessage());
	            }
	        }
	        return result;
	    }
	 
	 protected void beforeExecute(Connection conn) throws SQLException {
		 conn.setAutoCommit(autoCommit);
		 conn.setReadOnly(readOnly);
		 conn.setTransactionIsolation(transactionIsolation);
		 if(catalog != null) conn.setCatalog(catalog);
		 
	 }
	 
	 protected  void setParameters(PreparedStatement ps, Object[] params)
	 throws SQLException {
		 for (int i = 0; i< params.length; i++){
			 setValue(ps, params[i], i+1);
		 }
	 }
	
	 public Connection getCurrentConnection(){
		 return conn;
	 }
	 
	 private Connection getConnection() throws SQLException {
		 if(conn == null || conn.isClosed()){
			 conn = DriverManager.getConnection(prop.getProperty("jdbc.url")
					 ,prop.getProperty("jdbc.username")
					 ,prop.getProperty("jdbc.password"));
		 }
		 return conn;
	 }
	 
	 protected void setValue(PreparedStatement ps , Object value, int index)
	 throws SQLException {
		 if(value == null){
			 ps.setNull(index, java.sql.Types.NULL);
			 return ;
		 }
		 Class<?> type = value.getClass();
		 if(ClassUtils.isPrimitiveWrapper(type)){
			 type = ClassUtils.resolvePrimitiveClassName(type);
		 }
		 if(int.class.isAssignableFrom(type)){
			 ps.setInt(index, (Integer)value);
		 }else if (long.class.isAssignableFrom(type)){
			 ps.setLong(index, (Long)value);
		 }else if (boolean.class.isAssignableFrom(type)){
			 ps.setBoolean(index, (Boolean)value);
		 }else if (double.class.isAssignableFrom(type)){
			 ps.setDouble(index, (Double)value);
		 }else if (char.class.isAssignableFrom(type)){
			 ps.setString(index, (Character)value+"");
		 }else if (short.class.isAssignableFrom(type)){
			 ps.setShort(index, (Short)value);
		 }else if (float.class.isAssignableFrom(type)){
			 ps.setFloat(index, (Float)value);
		 }else if (byte.class.isAssignableFrom(type)){
			 ps.setByte(index, (Byte)value);
		 }else if (type.isArray()){
			 ps.setArray(index, (Array)value);
		 }else if(value instanceof  Timestamp){
			 ps.setTimestamp(index, (Timestamp)value);
		 }else if(value instanceof java.sql.Date){
			 ps.setDate(index, (java.sql.Date)value );
		 }else if(value instanceof java.util.Date){
			 ps.setDate(index, new java.sql.Date(((java.util.Date)value).getTime() ));
		 } else if (value instanceof Time){
			 ps.setTime(index, (Time)value);
		 }
	 }
	 
	 public static interface PreparedStatementCallback {
		 Object execute(Connection conn,PreparedStatement stmt) throws SQLException;
	 }
	 
	 public static interface ConnectionCallback{
		 Object execute(Connection conn) throws SQLException;
	 }
}
