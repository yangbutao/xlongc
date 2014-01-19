package org.yangbutao.longconn;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 线程对象，该线程读取响应并且通知caller对象。每一个connection拥有socket连接到远程地址。
 * 
 * @author yangbutao
 * 
 */
public class Connection extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(Connection.class);
	private ConnectionId remoteId;
	protected final SocketFactory socketFactory; // how to create sockets
	protected Socket socket = null;
	protected final boolean tcpNoDelay = false;
	protected final boolean tcpKeepAlive = true;
	protected DataInputStream in;
	protected DataOutputStream out;
	protected AtomicInteger counter = new AtomicInteger(0);

	// 当前处于活动状态的calls
	protected final ConcurrentSkipListMap<Integer, Call> calls = new ConcurrentSkipListMap<Integer, Call>();
	// 上一次IO操作的时间
	protected final AtomicLong lastActivity = new AtomicLong();
	// 标记该Connection关闭
	protected final AtomicBoolean shouldCloseConnection = new AtomicBoolean();
	protected IOException closeException; // close 原因

	protected int pingInterval; // how often sends ping to the server in msecs
	protected int socketTimeout = 20000; // socket timeout,默认20s
	final protected int maxIdleTime = 10000; // 连接最大空闲时间，超时将会被关闭，以防止长时间占用服务端资源
	final protected int maxRetries = 0;
	final static int DEFAULT_PING_INTERVAL = 60000; // 1 min 默认ping间隔
	final static int DEFAULT_SOCKET_TIMEOUT = 20000; // 20 seconds 默认socket超时时间
	final static int PING_CALL_ID = -1;

	public Connection(ConnectionId remoteId) throws IOException {
		this.remoteId = remoteId;
		this.pingInterval = DEFAULT_PING_INTERVAL;
		this.socketTimeout = DEFAULT_SOCKET_TIMEOUT;
		socketFactory = SocketFactory.getDefault();
		this.setDaemon(true);
	}

	public ConnectionId getRemoteId() {
		return remoteId;
	}

	public void setRemoteId(ConnectionId remoteId) {
		this.remoteId = remoteId;
	}

	/**
	 * 连接设置关闭标记或者因超时出现的异常直接抛出；否则ping服务端
	 * 
	 * @param e
	 * @throws IOException
	 */
	public void handleTimeout(SocketTimeoutException e) throws IOException {
		if (shouldCloseConnection.get() || remoteId.rpcTimeout > 0) {
			throw e;
		}
		sendPing();
	}

	/*
	 * Send a ping to the server if the time elapsed since last I/O activity is
	 * equal to or greater than the ping interval
	 */
	/**
	 * 按照pingInterval间隔(距离上次IO间隔已经达到pingInterval)发送ping 请求
	 * 
	 * @throws IOException
	 */
	protected synchronized void sendPing() throws IOException {
		long curTime = System.currentTimeMillis();
		if (curTime - lastActivity.get() >= pingInterval) {
			lastActivity.set(curTime);
			// noinspection SynchronizeOnNonFinalField
			synchronized (this.out) {
				out.writeInt(PING_CALL_ID);
				out.flush();
			}
		}
	}

	/**
	 * Add a call to this connection's call queue and notify a listener;
	 * synchronized. Returns false if called during shutdown.
	 * 
	 * @param call
	 *            to add
	 * @return true if the call was added.
	 */
	/**
	 * 
	 * @param call
	 * @return
	 */
	protected synchronized boolean addCall(Call call) {
		// 如果连接设置关闭标记，则不断重试
		if (shouldCloseConnection.get())
			return false;
		calls.put(call.id, call);
		// 通知接收线程，call队列由请求了，可以准备接收响应数据了，参见waitForWork
		notify();
		return true;
	}

	/**
	 * Connect to the server and set up the I/O streams. It then sends a header
	 * to the server and starts the connection thread that waits for responses.
	 * 
	 * @throws java.io.IOException
	 *             e
	 */
	/**
	 * 为当前连接，建立socket
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected synchronized void setupIOstreams() throws IOException,
			InterruptedException {
		// 如果socket已经建立或者当前连接需要关闭，则直接返回
		if (socket != null || shouldCloseConnection.get()) {
			return;
		}

		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Connecting to " + remoteId);
			}
			// 建立Connection的socket
			setupConnection();
			// 输入和输出流的初始化

			this.in = new DataInputStream(new BufferedInputStream(
					new PingInputStream(getInputStream(socket,
							socket.getSoTimeout()), this)));
			this.out = new DataOutputStream(new BufferedOutputStream(
					getOutputStream(socket, 0)));
			// writeHeader();

			// update last activity time
			touch();

			// start the receiver thread after the socket connection has been
			// set up
			start();
		} catch (IOException e) {
			// 因连接异常，而需要关闭该连接
			markClosed(e);
			close();

			throw e;
		}
	}

	protected void touch() {
		lastActivity.set(System.currentTimeMillis());
	}

	public static InputStream getInputStream(Socket socket, long timeout)
			throws IOException {
		return (socket.getChannel() == null) ? socket.getInputStream()
				: new SocketInputStream(socket, timeout);
	}

	public static OutputStream getOutputStream(Socket socket, long timeout)
			throws IOException {
		return (socket.getChannel() == null) ? socket.getOutputStream()
				: new SocketOutputStream(socket, timeout);
	}

	/**
	 * 设置关闭连接标记，后续对连接的请求可以依据该标记进行判断
	 * 
	 * @param e
	 */
	protected synchronized void markClosed(IOException e) {
		if (shouldCloseConnection.compareAndSet(false, true)) {
			closeException = e;
			notifyAll();
		}
	}

	/**
	 * call队列空(连接空闲)、连接出现异常，则返回false
	 * 
	 * @return
	 */
	protected synchronized boolean waitForWork() {
		// 当前连接比较空闲，那么就等待，直到超过最大空闲时间
		if (calls.isEmpty() && !shouldCloseConnection.get()) {
			long timeout = maxIdleTime
					- (System.currentTimeMillis() - lastActivity.get());
			if (timeout > 0) {
				try {
					// 等待，直到超时，或者队列有新的call触发notify
					wait(timeout);
				} catch (InterruptedException ignored) {
				}
			}
		}

		if (!calls.isEmpty() && !shouldCloseConnection.get()) {
			return true;
		} else if (shouldCloseConnection.get()) {
			return false;
		} else if (calls.isEmpty()) { // 连接空闲超过最大时间，关闭连接
			markClosed(null);
			return false;
		} else { // get stopped but there are still pending requests
			markClosed((IOException) new IOException()
					.initCause(new InterruptedException()));
			return false;
		}
	}

	protected void sendParam(Call call) {
		// 连接已经置为close标记，无需发送
		if (shouldCloseConnection.get()) {
			return;
		}

		// For serializing the data to be written.

		final DataOutputBuffer d = new DataOutputBuffer();
		try {
			if (LOG.isDebugEnabled())
				LOG.debug(getName() + " sending #" + call.id);

			// 数据格式：数据长度(4字节)|call_id(4字节)|数据
			d.writeInt(0xdeadbeef); // placeholder for data length
			d.writeInt(call.id);
			// TODO:写数据
			// call.param.write(d);
			d.write((byte[]) call.param);
			byte[] data = d.getData();
			int dataLength = d.getLength();
			// fill in the placeholder
			putInt(data, 0, dataLength - 4);
			// noinspection SynchronizeOnNonFinalField
			synchronized (this.out) { // FindBugs IS2_INCONSISTENT_SYNC
				out.write(data, 0, dataLength);
				out.flush();
			}
		} catch (IOException e) {
			markClosed(e);
		} finally {
			closeStream(d);
		}
	}

	public static int putInt(byte[] bytes, int offset, int val) {
		int SIZEOF_INT = Integer.SIZE / Byte.SIZE;
		if (bytes.length - offset < SIZEOF_INT) {
			throw new IllegalArgumentException(
					"Not enough room to put an int at" + " offset " + offset
							+ " in a " + bytes.length + " byte array");
		}
		for (int i = offset + 3; i > offset; i--) {
			bytes[i] = (byte) val;
			val >>>= 8;
		}
		bytes[offset] = (byte) val;
		return offset + SIZEOF_INT;
	}

	/**
	 * 在connection后台线程中，清理connection
	 */
	@Override
	public void run() {
		if (LOG.isDebugEnabled())
			LOG.debug(getName() + ": starting, having connections "
					+ ConnectionManager.INSTANCE.getConnections().size());

		try {
			// 在当前连接中循环接收响应数据，直到当前连接空闲下来
			while (waitForWork()) {
				receiveResponse();
			}
		} catch (Throwable t) {
			LOG.warn("Unexpected exception receiving call responses", t);
			markClosed(new IOException(
					"Unexpected exception receiving call responses", t));
		}

		close();

		if (LOG.isDebugEnabled())
			LOG.debug(getName() + ": stopped, remaining connections "
					+ ConnectionManager.INSTANCE.getConnections().size());
	}

	/**
	 * 回收当前连接，连接必须是置于close标记shouldCloseConnection=true
	 */
	protected synchronized void close() {
		// 如果没有因为异常等原因必须关闭connection，则直接返回，以重用该connection;否则从pool中清理该connection
		if (!shouldCloseConnection.get()) {// shouldCloseConnection=false
			LOG.error("The connection is not in the closed state");
			return;
		}

		// release the resources
		// first thing to do;take the connection out of the connection list
		synchronized (ConnectionManager.INSTANCE.getConnections()) {
			ConnectionManager.INSTANCE.getConnections().remove(remoteId, this);
		}

		// 关闭流，间接关闭socket
		closeStream(out);
		closeStream(in);

		// clean up all calls
		if (closeException == null) {
			if (!calls.isEmpty()) {
				LOG.warn("A connection is closed for no cause and calls are not empty");

				// clean up calls anyway
				closeException = new IOException("Unexpected closed connection");
				cleanupCalls();
			}
		} else {
			// log the info
			if (LOG.isDebugEnabled()) {
				LOG.debug("closing ipc connection to " + remoteId.address
						+ ": " + closeException.getMessage(), closeException);
			}

			// cleanup calls
			cleanupCalls();
		}
		if (LOG.isDebugEnabled())
			LOG.debug(getName() + ": closed");
	}

	/* Cleanup all calls and mark them as done */
	protected void cleanupCalls() {
		cleanupCalls(0);
	}

	public void closeStream(java.io.Closeable stream) {
		if (stream != null) {
			try {
				stream.close();
			} catch (IOException e) {
				if (LOG != null && LOG.isDebugEnabled()) {
					LOG.debug("Exception in closing " + stream, e);
				}
			}
		}
	}

	/*
	 * Receive a response. Because only one receiver, so no synchronization on
	 * in.
	 */
	protected void receiveResponse() {
		// 连接关闭，退出
		if (shouldCloseConnection.get()) {
			return;
		}
		touch();

		try {

			// 数据格式：call_id(4字节)|flag(1字节)|[长度(4字节)]|异常或者数据
			// 读取call id.
			int id = in.readInt();
			int datalength = 0;

			if (LOG.isDebugEnabled())
				LOG.debug(getName() + " got value #" + id);
			Call call = calls.remove(id);

			// Read the flag byte
			byte flag = in.readByte();
			boolean isError = ResponseFlag.isError(flag);
			if (ResponseFlag.isLength(flag)) {
				datalength = in.readInt();
			}
			if (isError) {
				if (call != null) {
					// TODO:异常信息反序列化，目前异常信息是字符串
					byte[] errorData = new byte[datalength];
					in.read(errorData);
					call.setException(new IOException(new String(errorData)));
				}
			} else {
				// TODO:数据反序列化
				byte[] realData = new byte[datalength];
				in.read(realData);
				// System.out.println(new String(realData));
				if (call != null) {//
					call.setValue(realData);
				}
			}
		} catch (IOException e) {
			if (e instanceof SocketTimeoutException && remoteId.rpcTimeout > 0) {
				closeException = e;
			} else {
				markClosed(e);
			}
		} finally {
			// 如果设置了rpc timeout，则需要清理超时的call
			if (remoteId.rpcTimeout > 0) {
				cleanupCalls(remoteId.rpcTimeout);
			}
		}
	}

	/**
	 * 清理超时的call请求
	 * 
	 * @param rpcTimeout
	 */
	protected void cleanupCalls(long rpcTimeout) {
		Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator();
		while (itor.hasNext()) {
			Call c = itor.next().getValue();
			long waitTime = System.currentTimeMillis() - c.getStartTime();
			if (waitTime >= rpcTimeout) {
				if (this.closeException == null) {
					this.closeException = new IOException("Call id=" + c.id
							+ ", waitTime=" + waitTime + ", rpcTimetout="
							+ rpcTimeout);
				}
				c.setException(this.closeException);
				synchronized (c) {
					c.notifyAll();
				}
				itor.remove();
			} else {
				break;
			}
		}
		try {
			if (!calls.isEmpty()) {
				Call firstCall = calls.get(calls.firstKey());
				long maxWaitTime = System.currentTimeMillis()
						- firstCall.getStartTime();
				if (maxWaitTime < rpcTimeout) {
					rpcTimeout -= maxWaitTime;
				}
			}
			if (!shouldCloseConnection.get()) {
				closeException = null;
				if (socket != null) {
					socket.setSoTimeout((int) rpcTimeout);
				}
			}
		} catch (SocketException e) {
			LOG.debug("Couldn't lower timeout, which may result in longer than expected calls");
		}
	}

	public static void connect(Socket socket, SocketAddress endpoint,
			int timeout) throws IOException {
		if (socket == null || endpoint == null || timeout < 0) {
			throw new IllegalArgumentException("Illegal argument for connect()");
		}

		SocketChannel ch = socket.getChannel();
		if (ch == null) {
			// 阻塞方式的IO
			socket.connect(endpoint, timeout);
		} else {
			// NIO
			SocketIOWithTimeout.connect(ch, endpoint, timeout);
		}
		if (socket.getLocalPort() == socket.getPort()
				&& socket.getLocalAddress().equals(socket.getInetAddress())) {
			socket.close();
			throw new ConnectException(
					"Localhost targeted connection resulted in a loopback. "
							+ "No daemon is listening on the target port.");
		}
	}

	/**
	 * 初始化socket连接
	 * 
	 * @throws IOException
	 */
	protected synchronized void setupConnection() throws IOException {
		short ioFailures = 0;
		short timeoutFailures = 0;
		while (true) {
			try {
				SocketChannel ch = SocketChannel.open();
				ch.configureBlocking(false);
				this.socket = ch.socket();
				// this.socket = socketFactory.createSocket();
				this.socket.setTcpNoDelay(tcpNoDelay);
				this.socket.setKeepAlive(tcpKeepAlive);
				// connection time out 默认 20s
				connect(this.socket, remoteId.getAddress(), socketTimeout);
				if (remoteId.rpcTimeout > 0) {
					pingInterval = remoteId.rpcTimeout;
				}
				this.socket.setSoTimeout(pingInterval);
				return;
			} catch (SocketTimeoutException toe) {
				handleConnectionFailure(timeoutFailures++, maxRetries, toe);
			} catch (IOException ie) {
				handleConnectionFailure(ioFailures++, maxRetries, ie);
			}
		}
	}

	/**
	 * 处理连接失败
	 * 
	 * @param curRetries
	 * @param maxRetries
	 * @param ioe
	 * @throws IOException
	 */
	private void handleConnectionFailure(int curRetries, int maxRetries,
			IOException ioe) throws IOException {

		closeConnection();

		if (curRetries >= maxRetries) {
			throw ioe;
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException ignored) {
		}

		LOG.info("Retrying connect to server: " + remoteId.getAddress()
				+ " after sleeping " + 1000 + "ms. Already tried " + curRetries
				+ " time(s).");
	}

	/**
	 * 关闭sokcet
	 */
	protected void closeConnection() {
		if (socket != null) {
			try {
				socket.close();
			} catch (IOException e) {
				LOG.warn("Not able to close a socket", e);
			}
		}
		socket = null;
	}

}
