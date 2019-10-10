package uk.ac.bristol.star.cdf.util;

import java.nio.ByteBuffer;

/**
 *
 * @author Christopher J. Weeks
 */
public class BigByteBuffer {

  private final long length;
  private final byte[][] byteArray;

  public static BigByteBuffer wrap(ByteBuffer[] array) {
    long arrayLength = 0;
    for (ByteBuffer buffer : array) {
      arrayLength += buffer.capacity();
    }

    return new BigByteBuffer(arrayLength);
  }

  public BigByteBuffer(long size) {
    this.byteArray = createArray(size);
    this.length = size;
  }

  public final byte[][] createArray(long length) {
    int topArrayLength = 0;
    int bottomArrayLength = 0;
    long total = length;
    while (total >= Integer.MAX_VALUE) {
      topArrayLength++;
      total -= Integer.MAX_VALUE;
    }
    bottomArrayLength = (int) total;

    return new byte[topArrayLength][bottomArrayLength];
  }

  public long capacity() {
    return length;
  }
}
