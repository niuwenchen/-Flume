package com.jackniu;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.bson.Document;
import org.bson.codecs.UuidCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jackniu.bean.LogBean;
import com.jackniu.utils.MongoDBJDBC;
import com.mongodb.MongoClientException;
import com.mongodb.client.MongoCollection;


public class LoggerSink extends AbstractSink implements Configurable{

	 private static Logger logger = LoggerFactory.getLogger(LoggerSink.class);
	 private static AtomicInteger counter = new AtomicInteger();
	 private static MongoCollection<Document> mongoCollection ;
	 
	 
	@Override
	public Status process(){
		logger.debug("{} start to process event", getName());
		
		Status status = Status.READY;
        try {
        	
            status = parseEvents();
           
        }catch(MongoClientException e){
        	System.out.println("****************MongoCLient 连接错误");
        }
        catch (Exception e) {
        	
            logger.error("can't process events", e);
        }
        logger.debug("{} processed event", getName());
        return status;
		
	}

	@Override
	public void configure(Context context) {
		logger.info("****************configure*******************");
		try {
			mongoCollection=(new MongoDBJDBC("192.168.31.127",27017 , "log_1")).getCollection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	private Status parseEvents()
	{
			Status status=null;
			Channel channel= getChannel();
			Transaction tx = channel.getTransaction();
			tx.begin();
			try{
				Event event =  channel.take();
				if(event==null)
				{
					tx.rollback();
					status=Status.BACKOFF;
					return status;
				}
				
				byte[] body = event.getBody();
				String str=new String(body);
				//logger.info("******"+str);
				
				
				// 增加MongoDB的插入
				logger.info("********************开始插入******");
				this.stormMongo(str);
				
				tx.commit();
				status=Status.READY;
				
			}catch(Exception e)
			{
				tx.rollback();
				status=Status.BACKOFF;
			}finally{
				tx.close();
			}
			return status;
		
	}
	
	public  void stormMongo(String str)
	{
		
		String[] arrs=str.split("\n");
//		LogBean bean = new LogBean(arrs[0], arrs[1], arrs[2], arrs[3]);
		Map<String,String>  map = new HashMap<String,String>();
		for(String arr:arrs)
		{
			map.put(UUID.randomUUID().toString(), arr);
			this.insertMongoDataToMmongo(map);
			map.clear();
		}
		
		
	}
	//插入数据到MongoDB数据库中
		public void insertMongoDataToMmongo(Map<String,String> mondodbmap)
			{
				Document insertData=new Document();
				for(String  key: mondodbmap.keySet())
				{
					insertData.put(key, mondodbmap.get(key));
				}			
				this.mongoCollection.insertOne(insertData);
				logger.info("********************插入成功******");
			}


}
