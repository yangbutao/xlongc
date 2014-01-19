/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yangbutao.longconn;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 支持socket channel的输入和输出流。并且流可以设置超时
 * 
 * @author yangbutao
 * 
 */
abstract class SocketIOWithTimeout {

	static final Logger LOG = LoggerFactory
			.getLogger(SocketIOWithTimeout.class);

	private SelectableChannel channel;
	private long timeout;
	private boolean closed = false;

	private static SelectorPool selector = new SelectorPool();

	SocketIOWithTimeout(SelectableChannel channel, long timeout)
			throws IOException {
		checkChannelValidity(channel);

		this.channel = channel;
		this.timeout = timeout;
		// 非阻塞
		channel.configureBlocking(false);
	}

	void close() {
		closed = true;
	}

	boolean isOpen() {
		return !closed && channel.isOpen();
	}

	SelectableChannel getChannel() {
		return channel;
	}

	/**
	 * 检查该channel是否ok。
	 * 
	 * @param channel
	 * @throws IOException
	 */
	static void checkChannelValidity(Object channel) throws IOException {
		if (channel == null) {
			throw new IOException("Channel is null. Check "
					+ "how the channel or socket is created.");
		}

		if (!(channel instanceof SelectableChannel)) {
			throw new IOException("Channel should be a SelectableChannel");
		}
	}

	/**
	 * 执行实际的IO操作，非阻塞的
	 * 
	 * @param buf
	 * @return
	 * @throws IOException
	 */
	abstract int performIO(ByteBuffer buf) throws IOException;

	/**
	 * 执行IO，返回读写的字节数。在超时时间内，channel不可读写就抛出SocketTimeoutException
	 * 
	 * @param buf
	 *            IO的buffer
	 * @param ops
	 *            Selection.OP_READ SelectionKey.OP_WRITE
	 * @return
	 * @throws IOException
	 */
	int doIO(ByteBuffer buf, int ops) throws IOException {

		if (!buf.hasRemaining()) {
			throw new IllegalArgumentException("Buffer has no data left.");
		}

		while (buf.hasRemaining()) {
			if (closed) {
				return -1;
			}

			try {
				int n = performIO(buf);
				if (n != 0) {
					return n;
				}
			} catch (IOException e) {
				if (!channel.isOpen()) {
					closed = true;
				}
				throw e;
			}

			// 等待socket ready
			int count = 0;
			try {
				count = selector.select(channel, ops, timeout);
			} catch (IOException e) { 
				closed = true;
				throw e;
			}

			if (count == 0) {
				throw new SocketTimeoutException(timeoutExceptionString(
						channel, timeout, ops));
			}
		}

		return 0; 
	}

	/**
	 * 类似于带超时的{@link SocketChannel#connect(SocketAddress)}
	 * 
	 * 
	 * @param channel
	 *            
	 * @param endpoint
	 * @throws IOException
	 */
	static void connect(SocketChannel channel, SocketAddress endpoint,
			int timeout) throws IOException {

		boolean blockingOn = channel.isBlocking();
		if (blockingOn) {
			channel.configureBlocking(false);
		}

		try {
			if (channel.connect(endpoint)) {
				return;
			}

			long timeoutLeft = timeout;
			long endTime = (timeout > 0) ? (System.currentTimeMillis() + timeout)
					: 0;

			while (true) {
				int ret = selector.select((SelectableChannel) channel,
						SelectionKey.OP_CONNECT, timeoutLeft);

				if (ret > 0 && channel.finishConnect()) {
					return;
				}

				if (ret == 0
						|| (timeout > 0 && (timeoutLeft = (endTime - System
								.currentTimeMillis())) <= 0)) {
					throw new SocketTimeoutException(timeoutExceptionString(
							channel, timeout, SelectionKey.OP_CONNECT));
				}
			}
		} catch (IOException e) {
			try {
				channel.close();
			} catch (IOException ignored) {
			}
			throw e;
		} finally {
			if (blockingOn && channel.isOpen()) {
				channel.configureBlocking(true);
			}
		}
	}

	/**
	 * 类似于{@link #doIO(ByteBuffer, int)} ，但是不执行IO操作。只是等待channel准备好IO
	 * 
	 * @param ops
	 *            Selection Ops 
	 * 
	 * @throws SocketTimeoutException
	 * @throws IOException
	 *             
	 */
	void waitForIO(int ops) throws IOException {

		if (selector.select(channel, ops, timeout) == 0) {
			throw new SocketTimeoutException(timeoutExceptionString(channel,
					timeout, ops));
		}
	}

	private static String timeoutExceptionString(SelectableChannel channel,
			long timeout, int ops) {

		String waitingFor;
		switch (ops) {

		case SelectionKey.OP_READ:
			waitingFor = "read";
			break;

		case SelectionKey.OP_WRITE:
			waitingFor = "write";
			break;

		case SelectionKey.OP_CONNECT:
			waitingFor = "connect";
			break;

		default:
			waitingFor = "" + ops;
		}

		return timeout + " millis timeout while "
				+ "waiting for channel to be ready for " + waitingFor
				+ ". ch : " + channel;
	}

	/**
	 *维护一个selectors 池。一旦空闲一段时间，这些selectors 就会被关闭
	 */
	private static class SelectorPool {

		private static class SelectorInfo {
			Selector selector;
			long lastActivityTime;
			LinkedList<SelectorInfo> queue;

			void close() {
				if (selector != null) {
					try {
						selector.close();
					} catch (IOException e) {
						LOG.warn("Unexpected exception while closing selector : "
								+ stringifyException(e));
					}
				}
			}
		}

		public static String stringifyException(Throwable e) {
			StringWriter stm = new StringWriter();
			PrintWriter wrt = new PrintWriter(stm);
			e.printStackTrace(wrt);
			wrt.close();
			return stm.toString();
		}

		private static class ProviderInfo {
			SelectorProvider provider;
			LinkedList<SelectorInfo> queue; // lifo
			ProviderInfo next;
		}

		private static final long IDLE_TIMEOUT = 10 * 1000; // 10 seconds.

		private ProviderInfo providerList = null;

		int select(SelectableChannel channel, int ops, long timeout)
				throws IOException {

			SelectorInfo info = get(channel);

			SelectionKey key = null;
			int ret = 0;

			try {
				while (true) {
					long start = (timeout == 0) ? 0 : System
							.currentTimeMillis();

					key = channel.register(info.selector, ops);
					ret = info.selector.select(timeout);

					if (ret != 0) {
						return ret;
					}

					if (timeout > 0) {
						timeout -= System.currentTimeMillis() - start;
						if (timeout <= 0) {
							return 0;
						}
					}

					if (Thread.currentThread().isInterrupted()) {
						throw new InterruptedIOException(
								"Interruped while waiting for "
										+ "IO on channel " + channel + ". "
										+ timeout + " millis timeout left.");
					}
				}
			} finally {
				if (key != null) {
					key.cancel();
				}

				try {
					info.selector.selectNow();
				} catch (IOException e) {
					LOG.info("Unexpected Exception while clearing selector : "
							+ stringifyException(e));
					info.close();
					return ret;
				}

				release(info);
			}
		}

		private synchronized SelectorInfo get(SelectableChannel channel)
				throws IOException {
			SelectorInfo selInfo = null;

			SelectorProvider provider = channel.provider();

			ProviderInfo pList = providerList;
			while (pList != null && pList.provider != provider) {
				pList = pList.next;
			}
			if (pList == null) {
				pList = new ProviderInfo();
				pList.provider = provider;
				pList.queue = new LinkedList<SelectorInfo>();
				pList.next = providerList;
				providerList = pList;
			}

			LinkedList<SelectorInfo> queue = pList.queue;

			if (queue.isEmpty()) {
				Selector selector = provider.openSelector();
				selInfo = new SelectorInfo();
				selInfo.selector = selector;
				selInfo.queue = queue;
			} else {
				selInfo = queue.removeLast();
			}

			trimIdleSelectors(System.currentTimeMillis());
			return selInfo;
		}

		private synchronized void release(SelectorInfo info) {
			long now = System.currentTimeMillis();
			trimIdleSelectors(now);
			info.lastActivityTime = now;
			info.queue.addLast(info);
		}

		private void trimIdleSelectors(long now) {
			long cutoff = now - IDLE_TIMEOUT;

			for (ProviderInfo pList = providerList; pList != null; pList = pList.next) {
				if (pList.queue.isEmpty()) {
					continue;
				}
				for (Iterator<SelectorInfo> it = pList.queue.iterator(); it
						.hasNext();) {
					SelectorInfo info = it.next();
					if (info.lastActivityTime > cutoff) {
						break;
					}
					it.remove();
					info.close();
				}
			}
		}
	}
}
