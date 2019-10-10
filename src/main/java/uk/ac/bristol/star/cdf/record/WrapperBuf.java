package uk.ac.bristol.star.cdf.record;

import java.io.IOException;
import java.io.InputStream;

/**
 * Buf implementation based on an existing Buf instance.
 * All methods are delegated to the base buf.
 *
 * @author Mark Taylor
 * @since 18 Jun 2013
 */
public class WrapperBuf implements Buf {

  private final Buf base_;

  /**
   * Constructor.
   *
   * @param base base buf
   */
  public WrapperBuf(final Buf base) {
    base_ = base;
  }

  @Override
  public long getLength() {
    return base_.getLength();
  }

  @Override
  public int readUnsignedByte(final Pointer ptr) throws IOException {
    return base_.readUnsignedByte(ptr);
  }

  @Override
  public int readInt(final Pointer ptr) throws IOException {
    return base_.readInt(ptr);
  }

  @Override
  public long readOffset(final Pointer ptr) throws IOException {
    return base_.readOffset(ptr);
  }

  @Override
  public String readAsciiString(final Pointer ptr, final int nbyte) throws IOException {
    return base_.readAsciiString(ptr, nbyte);
  }

  @Override
  public void setBit64(final boolean bit64) {
    base_.setBit64(bit64);
  }

  @Override
  public boolean isBit64() {
    return base_.isBit64();
  }

  @Override
  public void setEncoding(final boolean isBigendian) {
    base_.setEncoding(isBigendian);
  }

  @Override
  public boolean isBigendian() {
    return base_.isBigendian();
  }

  @Override
  public void readDataBytes(final long offset, final int count, final byte[] array)
    throws IOException {
    base_.readDataBytes(offset, count, array);
  }

  @Override
  public void readDataShorts(final long offset, final int count, final short[] array)
    throws IOException {
    base_.readDataShorts(offset, count, array);
  }

  @Override
  public void readDataInts(final long offset, final int count, final int[] array)
    throws IOException {
    base_.readDataInts(offset, count, array);
  }

  @Override
  public void readDataLongs(final long offset, final int count, final long[] array)
    throws IOException {
    base_.readDataLongs(offset, count, array);
  }

  @Override
  public void readDataFloats(final long offset, final int count, final float[] array)
    throws IOException {
    base_.readDataFloats(offset, count, array);
  }

  @Override
  public void readDataDoubles(final long offset, final int count, final double[] array)
    throws IOException {
    base_.readDataDoubles(offset, count, array);
  }

  @Override
  public InputStream createInputStream(final long offset) {
    return base_.createInputStream(offset);
  }

  @Override
  public Buf fillNewBuf(final long count, final InputStream in) throws IOException {
    return base_.fillNewBuf(count, in);
  }
}
