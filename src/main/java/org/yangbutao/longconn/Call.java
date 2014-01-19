package org.yangbutao.longconn;

import java.io.IOException;

/** 等待返回值得call对象 */
public class Call {
	int id; // call id
	byte[] param; // 輸入
	byte[] value; // 輸出
	IOException error; // 錯誤信息
	boolean done; // 返回响应时为true
	long startTime;

	protected Call(byte[] param) {
		this.param = param;
		this.startTime = System.currentTimeMillis();
		// this.id = conn.counter.incrementAndGet();
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	/**
	 * 表示call已经完成了，返回了数据或者错误信息。 并通知caller完成.
	 */
	protected synchronized void callComplete() {
		this.done = true;
		notify();
	}

	/**
	 * 设置错误信息，通知caller完成
	 * done.
	 * 
	 * @param error
	 *   
	 */
	public synchronized void setException(IOException error) {
		this.error = error;
		callComplete();
	}

	/**
	 * 如果没有错误信息，则设置返回的值。通知caller对象完成
	 * 
	 * @param value
	 *            
	 */
	public synchronized void setValue(byte[] value) {
		this.value = value;
		callComplete();
	}

	public long getStartTime() {
		return this.startTime;
	}
}