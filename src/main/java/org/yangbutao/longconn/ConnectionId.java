package org.yangbutao.longconn;

import java.net.InetSocketAddress;

/**
 * 保存有连接地址信息.客户端到服务端的连接由<remoteAddress，rpcTimeout>唯一标识别
 */
public class ConnectionId {

	final InetSocketAddress address;
	final int rpcTimeout;

	ConnectionId(InetSocketAddress address, int rpcTimeout) {
		this.address = address;
		this.rpcTimeout = rpcTimeout;
	}

	InetSocketAddress getAddress() {
		return address;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ConnectionId) {
			ConnectionId id = (ConnectionId) obj;
			return address.equals(id.address) && rpcTimeout == id.rpcTimeout;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return (address.hashCode()) ^ rpcTimeout;
	}

}
