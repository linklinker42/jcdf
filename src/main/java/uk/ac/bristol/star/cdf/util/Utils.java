package uk.ac.bristol.star.cdf.util;

import java.nio.ByteBuffer;

/**
 *
 * @author Christopher J. Weeks
 */
public class Utils {

  public static ByteBuffer toByteBuffer(final byte[] b, final int len) {
    return ByteBuffer.wrap(cp(b, len));
  }

  public static ByteBuffer toByteBuffer(final byte[] b) {
    return ByteBuffer.wrap(cp(b));
  }

  public static byte[] cp(final byte[] b, final int len) {
    final byte[] newArray = new byte[len];
    for (int i = 0; i < len; i++) {
      newArray[i] = b[i];
    }
    return newArray;
  }

  public static byte[] cp(final byte[] b) {
    return cp(b, b.length);
  }
}
