package com.jackniu.bean;

public class LogBean {
	public  String name;
	public String key1;
	public String key2;
	public String key3;
	
	
	public LogBean(String name, String key1, String key2, String key3) {
		super();
		this.name = name;
		this.key1 = key1;
		this.key2 = key2;
		this.key3 = key3;
	}
	
	
	@Override
	public String toString() {
		return "LogBean [name=" + name + ", key1=" + key1 + ", key2=" + key2 + ", key3=" + key3 + "]";
	}


	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getKey1() {
		return key1;
	}
	public void setKey1(String key1) {
		this.key1 = key1;
	}
	public String getKey2() {
		return key2;
	}
	public void setKey2(String key2) {
		this.key2 = key2;
	}
	public String getKey3() {
		return key3;
	}
	public void setKey3(String key3) {
		this.key3 = key3;
	}
	

}
