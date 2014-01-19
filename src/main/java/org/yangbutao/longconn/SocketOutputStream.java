package org.yangbutao.longconn;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.WritableByteChannel;

/**
 * 基于NIO的输出流
 * @author yangbutao
 *
 */
public class SocketOutputStream extends OutputStream implements
		WritableByteChannel {

	private Writer writer;

	private static class Writer extends SocketIOWithTimeout {
		WritableByteChannel channel;

		Writer(WritableByteChannel channel, long timeout) throws IOException {
			super((SelectableChannel) channel, timeout);
			this.channel = channel;
		}

		int performIO(ByteBuffer buf) throws IOException {
			return channel.write(buf);
		}
	}

	/**
	 * 创建一个给定timeout的输出流。如果timeout=0，则表示超时无限制时间。socket channel是无阻塞的
	 * @param channel
	 * @param timeout
	 * @throws IOException
	 */
	public SocketOutputStream(WritableByteChannel channel, long timeout)
			throws IOException {
		SocketIOWithTimeout.checkChannelValidity(channel);
		writer = new Writer(channel, timeout);
	}


	public SocketOutputStream(Socket socket, long timeout) throws IOException {
		this(socket.getChannel(), timeout);
	}

	public void write(int b) throws IOException {
		byte[] buf = new byte[1];
		buf[0] = (byte) b;
		write(buf, 0, 1);
	}

	public void write(byte[] b, int off, int len) throws IOException {
		ByteBuffer buf = ByteBuffer.wrap(b, off, len);
		while (buf.hasRemaining()) {
			try {
				if (write(buf) < 0) {
					throw new IOException("The stream is closed");
				}
			} catch (IOException e) {

				if (buf.capacity() > buf.remaining()) {
					writer.close();
				}
				throw e;
			}
		}
	}

	public synchronized void close() throws IOException {
		writer.channel.close();
		writer.close();
	}


	public WritableByteChannel getChannel() {
		return writer.channel;
	}


	public boolean isOpen() {
		return writer.isOpen();
	}

	public int write(ByteBuffer src) throws IOException {
		return writer.doIO(src, SelectionKey.OP_WRITE);
	}


	public void waitForWritable() throws IOException {
		writer.waitForIO(SelectionKey.OP_WRITE);
	}


	public void transferToFully(FileChannel fileCh, long position, int count)
			throws IOException {

		while (count > 0) {
			waitForWritable();
			int nTransfered = (int) fileCh.transferTo(position, count,
					getChannel());

			if (nTransfered == 0) {

				if (position >= fileCh.size()) {
					throw new EOFException("EOF Reached. file size is "
							+ fileCh.size() + " and " + count
							+ " more bytes left to be " + "transfered.");
				}
	
			} else if (nTransfered < 0) {
				throw new IOException("Unexpected return of " + nTransfered
						+ " from transferTo()");
			} else {
				position += nTransfered;
				count -= nTransfered;
			}
		}
	}

}
