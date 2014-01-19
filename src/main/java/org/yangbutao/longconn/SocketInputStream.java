package org.yangbutao.longconn;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

/**
 * 基于NIO的输入流
 * 
 * @author yangbutao
 * 
 */
public class SocketInputStream extends InputStream implements
		ReadableByteChannel {

	private Reader reader;

	private static class Reader extends SocketIOWithTimeout {
		ReadableByteChannel channel;

		Reader(ReadableByteChannel channel, long timeout) throws IOException {
			super((SelectableChannel) channel, timeout);
			this.channel = channel;
		}

		int performIO(ByteBuffer buf) throws IOException {
			return channel.read(buf);
		}
	}

	/**
	 * 创建一个给定timeout的输入流。如果timeout=0，则表示超时无限制时间。socket channel是无阻塞的
	 * 
	 * @param channel
	 * @param timeout
	 * @throws IOException
	 */
	public SocketInputStream(ReadableByteChannel channel, long timeout)
			throws IOException {
		SocketIOWithTimeout.checkChannelValidity(channel);
		reader = new Reader(channel, timeout);
	}

	/**
	 * 创建一个给定timeout的输入流。如果timeout=0，则表示超时无限制时间。socket channel是无阻塞的
	 * 
	 * @param socket
	 * @param timeout
	 * @throws IOException
	 */
	public SocketInputStream(Socket socket, long timeout) throws IOException {
		this(socket.getChannel(), timeout);
	}

	public SocketInputStream(Socket socket) throws IOException {
		this(socket.getChannel(), socket.getSoTimeout());
	}

	@Override
	public int read() throws IOException {
		byte[] buf = new byte[1];
		int ret = read(buf, 0, 1);
		if (ret > 0) {
			return (int) (buf[0] & 0xff);
		}
		if (ret != -1) {
			throw new IOException("Could not read from stream");
		}
		return ret;
	}

	public int read(byte[] b, int off, int len) throws IOException {
		return read(ByteBuffer.wrap(b, off, len));
	}

	public synchronized void close() throws IOException {
		// 因Socket.getInputStream().close()，关闭channel
		reader.channel.close();
		reader.close();
	}

	public ReadableByteChannel getChannel() {
		return reader.channel;
	}

	public boolean isOpen() {
		return reader.isOpen();
	}

	public int read(ByteBuffer dst) throws IOException {
		return reader.doIO(dst, SelectionKey.OP_READ);
	}


	/**
	 * 等待直到channel准备好读数据。可以指定超时时间
	 * @throws IOException
	 */
	public void waitForReadable() throws IOException {
		reader.waitForIO(SelectionKey.OP_READ);
	}

}
