package com.jackniu.utils;

import java.util.UUID;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

public class Test {

	public static void main(String[] args) throws Exception {
//		MongoDBJDBC mongojdbc=new MongoDBJDBC("127.0.0.1",27017 , "jacktestdb");
//		MongoCollection<Document> collection = mongojdbc.getCollection();
//		
//		long count =collection.count();
//		System.out.println(count);
		UUID uuid = UUID.randomUUID();
		System.out.println(uuid.toString());
		MongoDBJDBC jdbc = new MongoDBJDBC("127.0.0.1", 27017, "log_1");
		MongoCollection<Document> collection = jdbc.getCollection();
		long count = collection.count();
		System.out.println(count);
	
	}

}
