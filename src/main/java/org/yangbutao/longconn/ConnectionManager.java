package org.yangbutao.longconn;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;

import org.yangbutao.longconn.PoolMap.PoolType;

/**
 * 连接管理对象，通过该对象获取连接
 * @author yangbutao
 *
 */
public class ConnectionManager {
	public static ConnectionManager INSTANCE = new ConnectionManager();
	protected PoolMap<ConnectionId, Connection> connections;
	/**
	 * 连接池的大小
	 */
	private int poolSize = 10;

	public static void main(String[] args) throws Exception {
		byte[] param = "this is a test".getBytes();
		ConnectionManager.INSTANCE.call(param, new InetSocketAddress(
				"localhost", 9876), 20000);
	}

	public ConnectionManager() {
		connections = new PoolMap<ConnectionId, Connection>(
				PoolType.RoundRobin, poolSize);
	}

	/**
	 * 根据socketAddr，获取连接，连接维护在池中
	 * 
	 * @param addr
	 * @param rpcTimeout
	 * @param call
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected Connection getConnection(InetSocketAddress addr, int rpcTimeout,
			Call call) throws IOException, InterruptedException {
		Connection connection;
		ConnectionId remoteId = new ConnectionId(addr, rpcTimeout);
		do {
			synchronized (connections) {
				connection = connections.get(remoteId);
				if (connection == null) {
					connection = new Connection(remoteId);
					connections.put(remoteId, connection);
				}
			}
			call.setId(connection.counter.incrementAndGet());
		} while (!connection.addCall(call));

		// 如果连接已经建立，则不需要再setup io,需要判断socket是否为空
		connection.setupIOstreams();
		return connection;
	}

	public PoolMap<ConnectionId, Connection> getConnections() {
		return connections;
	}

	public void setConnections(PoolMap<ConnectionId, Connection> connections) {
		this.connections = connections;
	}

	/**
	 * 客户端同步调用的接口
	 * @param param 输入参数，已经序列化好的byte数组，该框架暂不提供序列化机制
	 * @param addr 远程服务端地址
	 * @param rpcTimeout rpc的超时时间
	 * @return 返回数据也是byte数组，该框架暂不提供反序列化的机制
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public byte[] call(byte[] param, InetSocketAddress addr, int rpcTimeout)
			throws InterruptedException, IOException {
		Call call = new Call(param);
		Connection connection = getConnection(addr, rpcTimeout, call);
		connection.sendParam(call); 
		boolean interrupted = false;
		synchronized (call) {
			while (!call.done) {
				try {
					call.wait(); // wait 结果
				} catch (InterruptedException ignored) {
					interrupted = true;
				}
			}

			if (interrupted) {
				Thread.currentThread().interrupt();
			}

			if (call.error != null) {
				if (call.error instanceof IOException) {
					call.error.fillInStackTrace();
					throw call.error;
				}
				throw wrapException(addr, call.error);
			}
			return call.value;
		}
	}

	protected IOException wrapException(InetSocketAddress addr,
			IOException exception) {
		if (exception instanceof ConnectException) {
			return (ConnectException) new ConnectException("Call to " + addr
					+ " failed on connection exception: " + exception)
					.initCause(exception);
		} else if (exception instanceof SocketTimeoutException) {
			return (SocketTimeoutException) new SocketTimeoutException(
					"Call to " + addr + " failed on socket timeout exception: "
							+ exception).initCause(exception);
		} else {
			return (IOException) new IOException("Call to " + addr
					+ " failed on local exception: " + exception)
					.initCause(exception);

		}
	}

}
