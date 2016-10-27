package com.jackniu.utils;

import java.net.UnknownHostException;
import java.util.Arrays;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;



public class MongoDBJDBC {
	private static String dbIp="";
	private static int port=0;
	private static final String userName="Jack";
	private static final String password="Jack";
	private static final String dbName="test";
	private static String dbTest="";
	
	
	private static MongoCollection<Document> collection;
	private static MongoCredential credential;
	private static MongoClient mongoClient;
	private static MongoDatabase database;
	
	public MongoDBJDBC(String dbIp,int port,String dbTest)
	{
		this.dbIp=dbIp;
		this.port=port;
		this.dbTest=dbTest;
	}
//	public MongoCollection<Document> getCollection() throws Exception{
//		try{
//			credential=MongoCredential.createScramSha1Credential(userName, dbName, password.toCharArray());
//			mongoClient = new MongoClient(new ServerAddress(this.dbIp+":"+this.port),Arrays.asList(credential));
//			database= mongoClient.getDatabase(dbName);
//			collection=database.getCollection(dbTest);
//		}catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//		return collection;
//	}
	
	public MongoCollection<Document> getCollection () throws UnknownHostException{  
	    try{
	    	credential=MongoCredential.createCredential(userName, dbName, password.toCharArray());
	    	mongoClient=new MongoClient(new ServerAddress(dbIp+":"+port),Arrays.asList(credential));
	    	database=mongoClient.getDatabase(dbName);
	    	collection=database.getCollection(dbTest);
	    }catch(Exception e) {  
            e.printStackTrace();  
	    }
		return collection;  
    }
	
	
	public static void  destroy()
	{
		if(mongoClient != null)
			mongoClient.close();
			mongoClient = null;
	      credential=null;
	      collection = null;
	      System.gc();
			
	}
	
}
