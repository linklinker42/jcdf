package uk.ac.bristol.star.cdf.record;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Buf implementation based on a single NIO ByteBuffer.
 * This works fine as long as it doesn't need to be more than 2^31 bytes (2Gb),
 * which is the maximum length of a ByteBuffer.
 *
 * @author Mark Taylor
 * @since 18 Jun 2013
 * @see java.nio.ByteBuffer
 */
public class SimpleNioBuf implements Buf {

  private final ByteBuffer byteBuf_;
  private final ByteBuffer dataBuf_;
  private boolean isBit64_;
  private boolean isBigendian_;

  /**
   * Constructor.
   *
   * @param byteBuf     NIO byte buffer containing the byte data
   * @param isBit64     64bit-ness of this buf
   * @param isBigendian true for big-endian, false for little-endian
   */
  public SimpleNioBuf(final ByteBuffer byteBuf, final boolean isBit64,
    final boolean isBigendian) {
    byteBuf_ = byteBuf;
    dataBuf_ = byteBuf.duplicate();
    setBit64(isBit64);
    setEncoding(isBigendian);
  }

  @Override
  public long getLength() {
    return byteBuf_.capacity();
  }

  @Override
  public int readUnsignedByte(final Pointer ptr) {
    return byteBuf_.get(toInt(ptr.getAndIncrement(1))) & 0xff;
  }

  @Override
  public int readInt(final Pointer ptr) {
    return byteBuf_.getInt(toInt(ptr.getAndIncrement(4)));
  }

  @Override
  public long readOffset(final Pointer ptr) {
    return isBit64_
      ? byteBuf_.getLong(toInt(ptr.getAndIncrement(8)))
      : (long) byteBuf_.getInt(toInt(ptr.getAndIncrement(4)));
  }

  @Override
  public String readAsciiString(final Pointer ptr, final int nbyte) {
    return Bufs.readAsciiString(byteBuf_,
      toInt(ptr.getAndIncrement(nbyte)),
      nbyte);
  }

  @Override
  public final synchronized void setBit64(final boolean isBit64) {
    isBit64_ = isBit64;
  }

  @Override
  public final synchronized void setEncoding(final boolean bigend) {

    // NIO buffers can do all the hard work - just tell them the
    // endianness of the data buffer.  Note however that the
    // endianness of control data is not up for grabs, so maintain
    // separate buffers for control data and application data.
    dataBuf_.order(bigend ? ByteOrder.BIG_ENDIAN
      : ByteOrder.LITTLE_ENDIAN);
    isBigendian_ = bigend;
  }

  @Override
  public boolean isBigendian() {
    return isBigendian_;
  }

  @Override
  public boolean isBit64() {
    return isBit64_;
  }

  @Override
  public void readDataBytes(final long offset, final int count, final byte[] array) {
    Bufs.readBytes(dataBuf_, toInt(offset), count, array);
  }

  @Override
  public void readDataShorts(final long offset, final int count, final short[] array) {
    Bufs.readShorts(dataBuf_, toInt(offset), count, array);
  }

  @Override
  public void readDataInts(final long offset, final int count, final int[] array) {
    Bufs.readInts(dataBuf_, toInt(offset), count, array);
  }

  @Override
  public void readDataLongs(final long offset, final int count, final long[] array) {
    Bufs.readLongs(dataBuf_, toInt(offset), count, array);
  }

  @Override
  public void readDataFloats(final long offset, final int count, final float[] array) {
    Bufs.readFloats(dataBuf_, toInt(offset), count, array);
  }

  @Override
  public void readDataDoubles(final long offset, final int count, final double[] array) {
    Bufs.readDoubles(dataBuf_, toInt(offset), count, array);
  }

  @Override
  public InputStream createInputStream(final long offset) {
    final ByteBuffer strmBuf = byteBuf_.duplicate();
    strmBuf.position((int) offset);
    return Bufs.createByteBufferInputStream(strmBuf);
  }

  @Override
  public Buf fillNewBuf(final long count, final InputStream in) throws IOException {
    int icount = toInt(count);
    final ByteBuffer bbuf = ByteBuffer.allocateDirect(icount);
    final ReadableByteChannel chan = Channels.newChannel(in);
    while (icount > 0) {
      final int nr = chan.read(bbuf);
      if (nr < 0) {
        throw new EOFException();
      } else {
        icount -= nr;
      }
    }
    return new SimpleNioBuf(bbuf, isBit64_, isBigendian_);
  }

  /**
   * Downcasts a long to an int.
   * If the value is too large, an unchecked exception is thrown.
   * That shouldn't happen because the only values this is invoked on
   * are offsets into a ByteBuffer.
   *
   * @param lvalue long value
   *
   * @return integer with the same value as <code>lvalue</code>
   */
  private static int toInt(final long lvalue) {
    final int ivalue = (int) lvalue;
    if (ivalue != lvalue) {
      throw new IllegalArgumentException("Pointer out of range: "
        + lvalue + " >32 bits");
    }
    return ivalue;
  }
}
