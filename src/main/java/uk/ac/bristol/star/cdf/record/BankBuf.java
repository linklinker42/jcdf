package uk.ac.bristol.star.cdf.record;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstract Buf implementation that divides the byte sequence into one
 * or more contiguous data banks.
 * Each bank contains a run of bytes short enough to be indexed by
 * a 4-byte integer.
 *
 * @author Mark Taylor
 * @since 18 Jun 2013
 */
public abstract class BankBuf implements Buf {

  private static final Logger LOGGER = Logger.getLogger(BankBuf.class.getName());

  private final long size_;
  private boolean isBit64_;
  private boolean isBigendian_;

  /**
   * Constructor.
   *
   * @param size        total size of buffer
   * @param isBit64     64bit-ness of buf
   * @param isBigendian true for big-endian data, false for little-endian
   */
  protected BankBuf(final long size, final boolean isBit64, final boolean isBigendian) {
    size_ = size;
    isBit64_ = isBit64;
    isBigendian_ = isBigendian;
  }

  /**
   * Returns the bank which can read a given number of bytes starting
   * at the given offset.
   *
   * <p>
   * Implementation: in most cases this will return one of the
   * large banks that this object has allocated.
   * However, in the case that the requested run straddles a bank
   * boundary it may be necessary to generate a short-lived bank
   * just to return from this method.
   *
   * @param offset start of required sequence
   * @param count  number of bytes in required sequence
   *
   * @return bank
   *
   * @throws java.io.IOException
   */
  protected abstract Bank getBank(final long offset, final int count)
    throws IOException;

  /**
   * Returns a list of active banks. Banks which have not been
   * created yet do not need to be included.
   *
   * @return
   */
  protected abstract List<Bank> getExistingBanks();

  /**
   * Returns an iterator over banks starting with the one containing
   * the given offset.
   * If followed to the end, the returned sequence
   * will go all the way to the end of the buf.
   * The first bank does not need to start at the
   * given offset, only to contain it.
   *
   * @param offset starting byte offset into buf
   *
   * @return iterator over data banks
   */
  protected abstract Iterator<Bank> getBankIterator(final long offset);

  @Override
  public long getLength() {
    return size_;
  }

  @Override
  public int readUnsignedByte(final Pointer ptr) throws IOException {
    final long pos = ptr.getAndIncrement(1);
    final Bank bank = getBank(pos, 1);
    return bank.byteBuffer_.get(bank.adjust(pos)) & 0xff;
  }

  @Override
  public int readInt(final Pointer ptr) throws IOException {
    final long pos = ptr.getAndIncrement(4);
    final Bank bank = getBank(pos, 4);
    return bank.byteBuffer_.getInt(bank.adjust(pos));
  }

  @Override
  public long readOffset(final Pointer ptr) throws IOException {
    final int nbyte = isBit64_ ? 8 : 4;
    final long pos = ptr.getAndIncrement(nbyte);
    final Bank bank = getBank(pos, nbyte);
    final int apos = bank.adjust(pos);
    return isBit64_ ? bank.byteBuffer_.getLong(apos)
      : (long) bank.byteBuffer_.getInt(apos);
  }

  @Override
  public String readAsciiString(final Pointer ptr, final int nbyte) throws IOException {
    final long offset = ptr.getAndIncrement(nbyte);
    final Bank bank = getBank(offset, nbyte);
    return Bufs.readAsciiString(bank.byteBuffer_, bank.adjust(offset),
      nbyte);
  }

  @Override
  public synchronized void setBit64(final boolean isBit64) {
    isBit64_ = isBit64;
  }

  @Override
  public boolean isBit64() {
    return isBit64_;
  }

  @Override
  public synchronized void setEncoding(final boolean bigend) {
    isBigendian_ = bigend;
    for (Bank bank : getExistingBanks()) {
      bank.setEncoding(isBigendian_);
    }
  }

  @Override
  public boolean isBigendian() {
    return isBigendian_;
  }

  @Override
  public void readDataBytes(final long offset, final int count, final byte[] array)
    throws IOException {
    final Bank bank = getBank(offset, count);
    Bufs.readBytes(bank.dataBuffer_, bank.adjust(offset), count, array);
  }

  @Override
  public void readDataShorts(final long offset, final int count, final short[] array)
    throws IOException {
    final Bank bank = getBank(offset, count * 2);
    Bufs.readShorts(bank.dataBuffer_, bank.adjust(offset),
      count, array);
  }

  @Override
  public void readDataInts(final long offset, final int count, final int[] array)
    throws IOException {
    final Bank bank = getBank(offset, count * 4);
    Bufs.readInts(bank.dataBuffer_, bank.adjust(offset), count, array);
  }

  @Override
  public void readDataLongs(final long offset, final int count, final long[] array)
    throws IOException {
    final Bank bank = getBank(offset, count * 8);
    Bufs.readLongs(bank.dataBuffer_, bank.adjust(offset), count, array);
  }

  @Override
  public void readDataFloats(final long offset, final int count, final float[] array)
    throws IOException {
    final Bank bank = getBank(offset, count * 4);
    Bufs.readFloats(bank.dataBuffer_, bank.adjust(offset),
      count, array);
  }

  @Override
  public void readDataDoubles(final long offset, final int count, final double[] array)
    throws IOException {
    final Bank bank = getBank(offset, count * 8);
    Bufs.readDoubles(bank.dataBuffer_, bank.adjust(offset),
      count, array);
  }

  @Override
  public InputStream createInputStream(final long offset) {
    final Iterator<Bank> bankIt = getBankIterator(offset);
    final Enumeration<InputStream> inEn = new Enumeration<InputStream>() {
      boolean isFirst = true;

      @Override
      public boolean hasMoreElements() {
        return bankIt.hasNext();
      }

      @Override
      public InputStream nextElement() {
        final Bank bank = bankIt.next();

        final ByteBuffer bbuf = bank.byteBuffer_.duplicate();
        bbuf.position(isFirst ? bank.adjust(offset) : 0);
        isFirst = false;
        return Bufs.createByteBufferInputStream(bbuf);
      }
    };
    return new SequenceInputStream(inEn);
  }

  @Override
  public Buf fillNewBuf(final long count, final InputStream in) throws IOException {
    return count <= Integer.MAX_VALUE
      ? fillNewSingleBuf((int) count, in)
      : fillNewMultiBuf(count, in);
  }

  /**
   * Implementation of fillNewBuf that works for small (&lt;2^31-byte)
   * byte sequences.
   *
   * @param count size of new buffer in bytes
   * @param in    input stream containing byte sequence
   *
   * @return buffer containing stream content
   */
  private Buf fillNewSingleBuf(int count, final InputStream in)
    throws IOException {

    // Memory is allocated outside of the JVM heap.
    final ByteBuffer bbuf = ByteBuffer.allocateDirect(count);
    final ReadableByteChannel chan = Channels.newChannel(in);
    while (count > 0) {
      final int nr = chan.read(bbuf);
      if (nr < 0) {
        throw new EOFException();
      } else {
        count -= nr;
      }
    }
    return Bufs.createBuf(bbuf, isBit64_, isBigendian_);
  }

  /**
   * Implementation of fillNewBuf that uses multiple ByteBuffers to
   * cope with large (&gt;2^31-byte) byte sequences.
   *
   * @param count size of new buffer in bytes
   * @param in    input stream containing byte sequence
   *
   * @return buffer containing stream content
   */
  private Buf fillNewMultiBuf(long count, final InputStream in)
    throws IOException {

    // Writes data to a temporary file.
    final File file = File.createTempFile("cdfbuf", ".bin");
    file.deleteOnExit();
    final int bufsiz = 64 * 1024;
    final byte[] buf = new byte[bufsiz];
    try (OutputStream out = new FileOutputStream(file)) {
      while (count > 0) {
        final int nr = in.read(buf);
        out.write(buf, 0, nr);
        count -= nr;
      }
    }
    return Bufs.createBuf(file, isBit64_, isBigendian_);
  }

  /**
   * Returns a BankBuf based on a single supplied ByteBuffer.
   *
   * @param byteBuffer  NIO buffer containing data
   * @param isBit64     64bit-ness of buf
   * @param isBigendian true for big-endian data, false for little-endian
   *
   * @return new buf
   */
  public static BankBuf createSingleBankBuf(final ByteBuffer byteBuffer,
    final boolean isBit64, final boolean isBigendian) {
    return new SingleBankBuf(byteBuffer, isBit64, isBigendian);
  }

  /**
   * Returns a BankBuf based on an array of supplied ByteBuffers.
   *
   * @param byteBuffers NIO buffers containing data (when concatenated)
   * @param isBit64     64bit-ness of buf
   * @param isBigendian true for big-endian data, false for little-endian
   *
   * @return new buf
   */
  public static BankBuf createMultiBankBuf(final ByteBuffer[] byteBuffers,
    final boolean isBit64, final boolean isBigendian) {
    return new PreMultiBankBuf(byteBuffers, isBit64, isBigendian);
  }

  /**
   * Returns a BankBuf based on supplied file channel.
   *
   * @param channel     readable file containing data
   * @param size        number of bytes in channel
   * @param bankSize    maximum size for individual data banks
   * @param isBit64     64bit-ness of buf
   * @param isBigendian true for big-endian data, false for little-endian
   *
   * @return new buf
   */
  public static BankBuf createMultiBankBuf(final FileChannel channel, final long size,
    final int bankSize, final boolean isBit64, final boolean isBigendian) {
    return new LazyMultiBankBuf(channel, size, bankSize,
      isBit64, isBigendian);
  }

  /**
   * BankBuf implementation based on a single NIO buffer.
   */
  private static class SingleBankBuf extends BankBuf {

    private final Bank bank_;

    /**
     * Constructor.
     *
     * @param byteBuffer  NIO buffer containing data
     * @param isBit64     64bit-ness of buf
     * @param isBigendian true for big-endian data,
     *                    false for little-endian
     */
    SingleBankBuf(final ByteBuffer byteBuffer, final boolean isBit64,
      final boolean isBigendian) {
      super(byteBuffer.capacity(), isBit64, isBigendian);
      bank_ = new Bank(byteBuffer, 0, isBigendian);
    }

    @Override
    public Bank getBank(final long offset, final int count) {
      return bank_;
    }

    @Override
    public List<Bank> getExistingBanks() {
      return Collections.singletonList(bank_);
    }

    @Override
    public Iterator<Bank> getBankIterator(final long offset) {
      return Collections.singletonList(bank_).iterator();
    }
  }

  /**
   * BankBuf implementation based on a supplied array of NIO buffers
   * representing contiguous subsequences of the data.
   */
  private static class PreMultiBankBuf extends BankBuf {

    private final Bank[] banks_;
    private final long[] starts_;
    private final long[] ends_;
    private int iCurrentBank_;

    /**
     * Constructor.
     *
     * @param byteBuffers NIO buffers containing data (when concatenated)
     * @param isBit64     64bit-ness of buf
     * @param isBigendian true for big-endian data,
     *                    false for little-endian
     */
    PreMultiBankBuf(final ByteBuffer[] byteBuffers, final boolean isBit64, final boolean isBigendian) {
      super(sumSizes(byteBuffers), isBit64, isBigendian);
      final int nbank = byteBuffers.length;
      banks_ = new Bank[nbank];
      starts_ = new long[nbank];
      ends_ = new long[nbank];
      long pos = 0L;
      for (int ibank = 0; ibank < nbank; ibank++) {
        final ByteBuffer byteBuffer = byteBuffers[ibank];
        banks_[ibank] = new Bank(byteBuffer, pos, isBigendian);
        starts_[ibank] = pos;
        pos += byteBuffer.capacity();
        ends_[ibank] = pos;
      }
      iCurrentBank_ = 0;
    }

    @Override
    protected Bank getBank(final long offset, int count) {
      // This is not synchronized, which means that the value of
      // iCurrentBank_ might be out of date (have been updated by
      // another thread).  It's OK not to defend against that,
      // since the out-of-date value would effectively just give
      // us a thread-local cached value, which is in fact an
      // advantage rather than otherwise.
      int ibank = iCurrentBank_;

      // Test if the most recently-used value is still correct
      // (usually it will be) and return it if so.
      if (offset >= starts_[ibank]
        && offset + count <= ends_[ibank]) {
        return banks_[ibank];
      } // Otherwise, find the bank corresponding to the requested offset.
      else {
        ibank = -1;
        for (int ib = 0; ib < banks_.length; ib++) {
          if (offset >= starts_[ib] && offset < ends_[ib]) {
            ibank = ib;
            break;
          }
        }

        // Update the cached value.
        iCurrentBank_ = ibank;

        // If it contains the whole requested run, return it.
        if (offset + count <= ends_[ibank]) {
          return banks_[ibank];
        } // Otherwise, the requested region straddles multiple banks.
        // This should be a fairly unusual occurrence.
        // Build a temporary bank to satisfy the request and return it.
        else {
          final byte[] tmp = new byte[count];
          int bankOff = (int) (offset - starts_[ibank]);
          int tmpOff = 0;
          int n = (int) (ends_[ibank] - offset);
          while (count > 0) {
            final ByteBuffer bbuf = banks_[ibank].byteBuffer_;
            synchronized (bbuf) {
              bbuf.position(bankOff);
              bbuf.get(tmp, tmpOff, n);
            }
            count -= n;
            tmpOff += n;
            bankOff = 0;
            ibank++;
            n = (int) Math.min(count,
              ends_[ibank] - starts_[ibank]);
          }
          return new Bank(ByteBuffer.wrap(tmp), offset,
            isBigendian());
        }
      }
    }

    @Override
    public List<Bank> getExistingBanks() {
      return Arrays.asList(banks_);
    }

    @Override
    public Iterator<Bank> getBankIterator(final long offset) {
      final Iterator<Bank> it = Arrays.asList(banks_).iterator();
      for (int ib = 0; ib < banks_.length; ib++) {
        if (offset >= starts_[ib] && offset <= ends_[ib]) {
          return it;
        }
        it.next();
      }
      return it;  // empty
    }

    /**
     * Returns the sum of the sizes of all the elements of a supplied array
     * of NIO buffers.
     *
     * @param byteBuffers buffer array
     *
     * @return number of bytes in concatenation of all buffers
     */
    private static long sumSizes(final ByteBuffer[] byteBuffers) {
      long size = 0;
      for (ByteBuffer byteBuffer : byteBuffers) {
        size += byteBuffer.capacity();
      }
      return size;
    }
  }

  /**
   * BankBuf implementation that uses multiple data banks,
   * but constructs (maps) them lazily as required.
   * The original data is supplied in a FileChannel.
   * All banks except (probably) the final one are the same size,
   * supplied at construction time.
   */
  private static class LazyMultiBankBuf extends BankBuf {

    private final FileChannel channel_;
    private final long size_;
    private final long bankSize_;
    private final Bank[] banks_;

    /**
     * Constructor.
     *
     * @param channel     readable file containing data
     * @param size        number of bytes in channel
     * @param bankSize    maximum size for individual data banks
     * @param isBit64     64bit-ness of buf
     * @param isBigendian true for big-endian data,
     *                    false for little-endian
     */
    LazyMultiBankBuf(final FileChannel channel, final long size, final int bankSize,
      final boolean isBit64, final boolean isBigendian) {
      super(size, isBit64, isBigendian);
      channel_ = channel;
      size_ = size;
      bankSize_ = bankSize;
      final int nbank = (int) (((size - 1) / bankSize) + 1);
      banks_ = new Bank[nbank];
    }

    @Override
    public Bank getBank(final long offset, int count) throws IOException {
      // Find out the index of the bank containing the starting offset.
      int ibank = (int) (offset / bankSize_);

      // If the requested read amount is fully contained in that bank,
      // lazily obtain and return it.
      final int over = (int) (offset + count - (ibank + 1) * bankSize_);
      if (over <= 0) {
        return getBankByIndex(ibank);
      } // Otherwise, the requested region straddles multiple banks.
      // This should be a fairly unusual occurrence.
      // Build a temporary bank to satisfy the request and return it.
      else {
        final byte[] tmp = new byte[count];
        int bankOff = (int) (bankSize_ - count + over);
        int tmpOff = 0;
        int n = count - over;
        while (count > 0) {
          final ByteBuffer bbuf = getBankByIndex(ibank).byteBuffer_;
          synchronized (bbuf) {
            bbuf.position(bankOff);
            bbuf.get(tmp, tmpOff, n);
          }
          count -= n;
          tmpOff += n;
          bankOff = 0;
          ibank++;
          n = (int) Math.min(count, bankSize_);
        }
        return new Bank(ByteBuffer.wrap(tmp), offset,
          isBigendian());
      }
    }

    @Override
    public List<Bank> getExistingBanks() {
      final List<Bank> list = new ArrayList<>();
      for (Bank bank : banks_) {
        if (bank != null) {
          list.add(bank);
        }
      }
      return list;
    }

    @Override
    public Iterator<Bank> getBankIterator(final long offset) {

      return new Iterator<Bank>() {
        int ibank = (int) (offset / bankSize_);

        @Override
        public boolean hasNext() {
          return ibank < banks_.length;
        }

        @Override
        public Bank next() {
          try {
            return getBankByIndex(ibank++);
          } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error acquiring bank", e);
            return null;
          }
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    /**
     * Lazily obtains and returns a numbered bank. Will not return null.
     *
     * @param ibank bank index
     */
    private Bank getBankByIndex(final int ibank) throws IOException {
      if (banks_[ibank] == null) {
        final long start = ibank * bankSize_;
        final long end = Math.min(((ibank + 1) * bankSize_), size_);
        final int leng = (int) (end - start);
        final ByteBuffer bbuf = channel_.map(FileChannel.MapMode.READ_ONLY, start, leng);
        banks_[ibank] = new Bank(bbuf, start, isBigendian());
      }
      return banks_[ibank];
    }
  }

  /**
   * Data bank for use within BankBuf class and its subclasses.
   * This stores a subsequence of bytes for the Buf, and records
   * its position within the whole sequence.
   */
  protected static class Bank {

    /**
     * Raw buffer.
     */
    private final ByteBuffer byteBuffer_;

    /**
     * Buffer adjusted for endianness.
     */
    private final ByteBuffer dataBuffer_;

    private final long start_;
    private final int size_;

    @Override
    public String toString() {
      return "Bank{" + "start_=" + start_ + ", size_=" + size_ + '}';
    }

    /**
     * Constructor.
     *
     * @param byteBuffer  NIO buffer containing data
     * @param start       offset into the full sequence at which this bank
     *                    is considered to start
     * @param isBigendian true for big-endian, false for little-endian
     */
    public Bank(final ByteBuffer byteBuffer, final long start, final boolean isBigendian) {
      byteBuffer_ = byteBuffer;
      dataBuffer_ = byteBuffer.duplicate();
      start_ = start;
      size_ = byteBuffer.capacity();
      setEncoding(isBigendian);
    }

    /**
     * Returns the position within this bank's buffer that corresponds
     * to an offset into the full byte sequence.
     *
     * @param pos offset into Buf
     *
     * @return pos - start
     *
     * @throws IllegalArgumentException pos is not between start and
     *                                  start+size
     */
    private int adjust(final long pos) {
      final long offset = pos - start_;
      if (offset >= 0 && offset < size_) {
        return (int) offset;
      } else {
        throw new IllegalArgumentException("Out of range: " + pos
          + " for bank at " + start_);
      }
    }

    /**
     * Resets the endianness for the data buffer of this bank.
     *
     * @param isBigendian true for big-endian, false for little-endian
     */
    private void setEncoding(final boolean isBigendian) {
      dataBuffer_.order(isBigendian ? ByteOrder.BIG_ENDIAN
        : ByteOrder.LITTLE_ENDIAN);
    }
  }
}
