package org.yangbutao.longconn;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;

public class PingInputStream  extends FilterInputStream {
	private Connection connection;
   
    public PingInputStream(InputStream in,Connection connection) {
      super(in);
      this.connection=connection;
    }



    /** 从流中读取一个字节
     * 超时的话，发送一个ping。如果没有错误就重试知道读到一个byte
     * 
     * @throws IOException 除了socket timeout的异常就抛出
     */
    @Override
    public int read() throws IOException {
      do {
        try {
          return super.read();
        } catch (SocketTimeoutException e) {
          connection.handleTimeout(e);
        }
      } while (true);
    }

    /** 从流off位置读取len长度的字节到buffer中，
     * 超时的话，发送一个ping。如果没有错误就重试知道读到
     * 
     * @throws IOException 除了socket timeout的异常就抛出
     */
    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
      do {
        try {
          return super.read(buf, off, len);
        } catch (SocketTimeoutException e) {
          connection.handleTimeout(e);
        }
      } while (true);
    }

}
