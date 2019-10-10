package uk.ac.bristol.star.cdf.record;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 *
 * @author Christopher J. Weeks
 */
public class NewBuf implements Buf {

  private final ByteBuffer[] byteBuf_;
  private final ByteBuffer[] dataBuf_;
  private boolean isBit64_;
  private boolean isBigendian_;

  public NewBuf(ByteBuffer[] byteBuf, boolean isBit64,
    boolean isBigendian) {
    byteBuf_ = byteBuf;
    dataBuf_ = duplicate(byteBuf);
    setBit64(isBit64);
    setEncoding(isBigendian);
  }

  public final ByteBuffer[] duplicate(ByteBuffer[] buffers) {
    ByteBuffer[] newBuffers = new ByteBuffer[buffers.length];
    for (int i = 0; i < buffers.length; i++) {
      newBuffers[i] = buffers[i].duplicate();
    }

    return newBuffers;
  }

  @Override
  public long getLength() {
    long size = 0;
    for (ByteBuffer byteBuffer : byteBuf_) {
      size += byteBuffer.capacity();
    }

    return size;
  }

  private static int toInt(long lvalue) {
    int ivalue = (int) lvalue;
    if (ivalue != lvalue) {
      throw new IllegalArgumentException("Pointer out of range: "
        + lvalue + " >32 bits");
    }
    return ivalue;
  }

  public int findInt(ByteBuffer[] buffers, int index) {
    int size = 0;
    int currentIndex = index;
    for (ByteBuffer buffer : buffers) {
      size += buffer.capacity();

      if (index <= size) {
        return buffer.getInt(currentIndex);
      }

      currentIndex -= buffer.capacity();
    }

    throw new RuntimeException("Could not find byte.");
  }

  public long findLong(ByteBuffer[] buffers, int index) {
    int size = 0;
    int currentIndex = index;
    for (ByteBuffer buffer : buffers) {
      size += buffer.capacity();

      if (index <= size) {
        return buffer.getLong(currentIndex);
      }

      currentIndex -= buffer.capacity();
    }

    throw new RuntimeException("Could not find byte.");
  }

  public byte findByte(ByteBuffer[] buffers, int index) {
    int size = 0;
    int currentIndex = index;
    for (ByteBuffer buffer : buffers) {
      size += buffer.capacity();

      if (index <= size) {
        return buffer.get(currentIndex);
      }

      currentIndex -= buffer.capacity();
    }

    throw new RuntimeException("Could not find byte.");
  }

  public short findShort(ByteBuffer[] buffers, int index) {
    int size = 0;
    int currentIndex = index;
    for (ByteBuffer buffer : buffers) {
      size += buffer.capacity();

      if (index <= size) {
        System.out.println("BufferSize=" + buffer.capacity());
        System.out.println("size=" + size);
        System.out.println("currentIndex=" + currentIndex);
        return buffer.getShort(currentIndex);
      }

      currentIndex -= buffer.capacity();
    }

    throw new RuntimeException("Could not find byte.");
  }

  @Override
  public int readUnsignedByte(Pointer ptr) throws IOException {
    return findByte(byteBuf_, toInt(ptr.getAndIncrement(1)));
  }

  @Override
  public int readInt(Pointer ptr) throws IOException {
    return findInt(byteBuf_, toInt(ptr.getAndIncrement(4)));
  }

  @Override
  public long readOffset(Pointer ptr) throws IOException {
    return isBit64_
      ? findLong(byteBuf_, toInt(ptr.getAndIncrement(8)))
      : (long) findInt(byteBuf_, toInt(ptr.getAndIncrement(4)));
  }

  @Override
  public String readAsciiString(Pointer ptr, int nbyte) throws IOException {
    return readAsciiString(byteBuf_,
      toInt(ptr.getAndIncrement(nbyte)),
      nbyte);
  }

  String readAsciiString(ByteBuffer[] buffers, int offset, int length) {
    StringBuffer sbuf = new StringBuffer(length);

    for (int i = 0; i < length; i++) {
      byte b = findByte(buffers, offset + i);
      if (b == 0) {
        break;
      } else {
        sbuf.append((char) b);
      }
    }
    return sbuf.toString();
  }

  @Override
  public void setBit64(boolean isBit64) {
    isBit64_ = isBit64;
  }

  @Override
  public boolean isBit64() {
    return isBit64_;
  }

  @Override
  public void setEncoding(boolean isBigendian) {
    for (ByteBuffer buffer : dataBuf_) {
      buffer.order(isBigendian ? ByteOrder.BIG_ENDIAN
        : ByteOrder.LITTLE_ENDIAN);
    }
    isBigendian_ = isBigendian;
  }

  @Override
  public boolean isBigendian() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void readDataBytes(long offset, int count, byte[] array) throws IOException {
    readBytes(dataBuf_, toInt(offset), count, array);
  }

  void readBytes(ByteBuffer[] bbuf, int ioff, int count, byte[] a) {
    if (count == 1) {
      a[0] = findByte(bbuf, ioff);
    } else {
      synchronized (bbuf) {
        for (int i = 0; i < count; i++) {
          a[i] = findByte(bbuf, ioff + i);
        }
      }
    }
  }

  @Override
  public void readDataShorts(long offset, int count, short[] array) throws IOException {
    readShorts(dataBuf_, toInt(offset), count, array);
  }

  void readShorts(ByteBuffer[] bbuf, int ioff, int count, short[] a) {
    if (count == 1) {
      a[0] = findShort(bbuf, ioff);
    } else {
      synchronized (bbuf) {
        for (int i = 0; i < count; i++) {
          a[i] = findShort(bbuf, ioff + i);
        }
      }
    }
  }

  @Override
  public void readDataInts(long offset, int count, int[] array) throws IOException {
    readInts(dataBuf_, toInt(offset), count, array);
  }

  void readInts(ByteBuffer[] bbuf, int ioff, int count, int[] a) {
    if (count == 1) {
      a[0] = findInt(bbuf, ioff);
    } else {
      synchronized (bbuf) {
        for (int i = 0; i < count; i++) {
          a[i] = findInt(bbuf, ioff + i);
        }
      }
    }
  }

  @Override
  public void readDataLongs(long offset, int count, long[] array) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void readDataFloats(long offset, int count, float[] array) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void readDataDoubles(long offset, int count, double[] array) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public InputStream createInputStream(long offset) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Buf fillNewBuf(long count, InputStream in) throws IOException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
