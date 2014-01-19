package org.yangbutao.longconn;

/**
 * 响应数据中有一个byte位表示返回的是错误信息还是正常数据
 * @author yangbutao
 * 
 */
public class ResponseFlag {
	private static final byte ERROR_BIT = 0x1;
	private static final byte LENGTH_BIT = 0x2;

	private ResponseFlag() {

	}

	static boolean isError(final byte flag) {
		return (flag & ERROR_BIT) != 0;
	}

	static boolean isLength(final byte flag) {
		return (flag & LENGTH_BIT) != 0;
	}

	static byte getLengthSetOnly() {
		return LENGTH_BIT;
	}

	static byte getErrorAndLengthSet() {
		return LENGTH_BIT | ERROR_BIT;
	}
}