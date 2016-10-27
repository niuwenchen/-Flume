package com.changhong.bigdata.flume.nginxmonitor.mongodb.sink;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.Sink.Status;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.bson.Document;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.jackniu.LoggerSink;
import com.jackniu.utils.MongoDBJDBC;
import com.mongodb.client.MongoCollection;

import net.sf.json.JSONObject;

public class MongoNginxSink extends AbstractSink implements Configurable{

	 private static Logger logger = LoggerFactory.getLogger(LoggerSink.class);
	 private static AtomicInteger counter = new AtomicInteger();
	 private static MongoDBJDBC connectMg;
	 private static MongoCollection<Document> mongoCollection ;
	 private int batchSize;
	 private String host;
	 private int port;
	 private String dbTest;
	
	 
	 @Override
	public void configure(Context context) {
			logger.info("****************configure*******************");
			try {
				batchSize = context.getInteger("batchSize");
				host = context.getString("host");
				port = context.getInteger("port");
				dbTest = context.getString("dbTest");
				
				Preconditions.checkArgument(StringUtils.isNotBlank(host),"Missing Param: host");
				Preconditions.checkArgument(port>0,"Missing Param: host");
				Preconditions.checkArgument(StringUtils.isNotBlank(dbTest),"Missing Param: host");
				
	
				this.connectMg=new MongoDBJDBC(host, port, dbTest);
				mongoCollection=this.connectMg.getCollection();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	
	@Override
	public synchronized void stop() {
		this.connectMg.destroy();
	}

	@Override
	public Status process() throws EventDeliveryException {
		 logger.debug("{} start to process event", getName());

	     Status status = Status.READY;
	     try {
	          status = parseEvents();
	     } catch (Exception e) {
	          logger.error("can't process events", e);
	     }
	     logger.debug("{} processed event", getName());
	     return status;
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
						
					// 增加MongoDB的插入
					logger.info("********************开始插入******");
					this.storeMongo(str);
					
					tx.commit();
					status=Status.READY;

				}catch (Exception e) {
		            logger.error("can't process events, drop it!", e);
		            if (tx != null) {
		                tx.commit();// commit to drop bad event, otherwise it will enter dead loop.
		            }
		        } finally {
		            if (tx != null) {
		                tx.close();
		            }
		        }
			return status;
		
	}
	
	public  void storeMongo(String str)
	{
		
		
		Map<String,String>  map=JSONObject.fromObject(str);
		Map<String,String> tempMap=new HashMap<String,String>();
		for(String keystr:map.keySet())
		{
			tempMap.put(keystr, map.get(keystr));
		}
		this.insertMongoDataToMmongo(tempMap);
		tempMap.clear();	
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
