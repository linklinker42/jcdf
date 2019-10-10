
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import uk.ac.bristol.star.cdf.record.BankBuf;
import uk.ac.bristol.star.cdf.record.Buf;
import uk.ac.bristol.star.cdf.record.Pointer;
import uk.ac.bristol.star.cdf.record.SimpleNioBuf;

public class BufTest {

  private final int blk_ = 54;
  private final int nn_ = 64;

  // Puts the various Buf implementations through their paces.
  @Test
  public void testBufs() throws Exception {
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (DataOutputStream dout = new DataOutputStream(bout)) {
      for (int i = 0; i < nn_; i++) {
        dout.writeByte(-i);
        dout.writeByte(i);
        dout.writeShort(-i);
        dout.writeShort(i);
        dout.writeInt(-i);
        dout.writeInt(i);
        dout.writeLong(-i);
        dout.writeLong(i);
        dout.writeFloat(-i);
        dout.writeFloat(i);
        dout.writeDouble(-i);
        dout.writeDouble(i);
      }
      dout.flush();
    }
    final byte[] bytes = bout.toByteArray();
    final int nbyte = bytes.length;
    assertEquals(blk_ * nn_, nbyte);

    final boolean isBit64 = false;
    final boolean isBigEndian = true;
    final ByteBuffer buf1 = ByteBuffer.wrap(bytes);
    checkBuf(new SimpleNioBuf(buf1, isBit64, isBigEndian));
    checkBuf(BankBuf.createSingleBankBuf(buf1, isBit64, isBigEndian));
    checkBuf(BankBuf.createMultiBankBuf(new ByteBuffer[]{buf1},
      isBit64, isBigEndian));

    final int[] banksizes
      = {23, blk_ - 1, blk_ + 1, 49, blk_ * 4, blk_ * 2 + 2};
    final List<ByteBuffer> bblist = new ArrayList<>();
    int ioff = 0;
    int ibuf = 0;
    int nleft = nbyte;
    while (nleft > 0) {
      final int leng = Math.min(banksizes[ibuf % banksizes.length], nleft);
      final byte[] bb = new byte[leng];
      System.arraycopy(bytes, ioff, bb, 0, leng);
      bblist.add(ByteBuffer.wrap(bb));
      ibuf++;
      ioff += leng;
      nleft -= leng;
    }
    final ByteBuffer[] bbufs = bblist.toArray(new ByteBuffer[0]);
    assertTrue(bbufs.length > 6);
    checkBuf(BankBuf
      .createMultiBankBuf(bbufs, isBit64, isBigEndian));

    final File tmpFile = File.createTempFile("data", ".bin");
    tmpFile.deleteOnExit();
    try (FileOutputStream fout = new FileOutputStream(tmpFile)) {
      fout.write(bytes);
    }
    try (FileChannel inchan = new FileInputStream(tmpFile).getChannel()) {
      final int[] banksizes2 = new int[banksizes.length + 2];
      System.arraycopy(banksizes, 0, banksizes2, 0, banksizes.length);
      banksizes2[banksizes.length + 0] = nbyte;
      banksizes2[banksizes.length + 1] = nbyte * 2;
      for (int banksize : banksizes2) {
        checkBuf(BankBuf.createMultiBankBuf(inchan, nbyte, banksize,
          isBit64, isBigEndian));
      }
    }

    final ByteBuffer copybuf;
    try (FileChannel copychan = new FileInputStream(tmpFile).getChannel()) {
      assertEquals(copychan.size(), nbyte);
      copybuf = ByteBuffer.allocate(nbyte);
      copychan.read(copybuf);
    }
    assertEquals(copybuf.position(), nbyte);
    checkBuf(new SimpleNioBuf(copybuf, isBit64, isBigEndian));

    tmpFile.delete();
  }

  public void checkBuf(final Buf buf) throws Exception {
    assertEquals(nn_ * blk_, buf.getLength());
    final byte[] abytes = new byte[2];
    final short[] ashorts = new short[2];
    final int[] aints = new int[4];
    final long[] alongs = new long[2];
    final float[] afloats = new float[21];
    final double[] adoubles = new double[2];
    for (int i = 0; i < nn_; i++) {
      final int ioff = i * blk_;
      buf.readDataBytes(ioff + 0, 2, abytes);
      buf.readDataShorts(ioff + 2, 2, ashorts);
      buf.readDataInts(ioff + 6, 2, aints);
      buf.readDataLongs(ioff + 14, 2, alongs);
      buf.readDataFloats(ioff + 30, 2, afloats);
      buf.readDataDoubles(ioff + 38, 2, adoubles);
      assertEquals(-i, abytes[0]);
      assertEquals(i, abytes[1]);
      assertEquals(-i, ashorts[0]);
      assertEquals(i, ashorts[1]);
      assertEquals(-i, aints[0]);
      assertEquals(i, aints[1]);
      assertEquals(-i, alongs[0]);
      assertEquals(i, alongs[1]);
      assertEquals(-i, afloats[0], 0);
      assertEquals(i, afloats[1], 0);
      assertEquals(-i, adoubles[0], 0);
      assertEquals(i, adoubles[1], 0);
    }
    final Pointer p = new Pointer(0);
    assertEquals(0, buf.readUnsignedByte(p));
    assertEquals(0, buf.readUnsignedByte(p));
    p.set(blk_);
    assertEquals(255, buf.readUnsignedByte(p));
    assertEquals(1, buf.readUnsignedByte(p));
  }

}
