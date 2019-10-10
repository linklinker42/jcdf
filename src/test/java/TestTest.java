
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import uk.ac.bristol.star.cdf.AttributeEntry;
import uk.ac.bristol.star.cdf.CdfContent;
import uk.ac.bristol.star.cdf.CdfReader;
import uk.ac.bristol.star.cdf.EpochFormatter;
import uk.ac.bristol.star.cdf.GlobalAttribute;
import uk.ac.bristol.star.cdf.Variable;
import uk.ac.bristol.star.cdf.VariableAttribute;
import uk.ac.bristol.star.cdf.record.Buf;
import uk.ac.bristol.star.cdf.record.Bufs;

/**
 *
 * @author Christopher J. Weeks
 */
public class TestTest {

  @Test
  public void testExample1() throws Exception {
    final File file = new File("example1.cdf");
    final CdfContent content = new CdfContent(new CdfReader(file));
    testExample1(content);
  }

  public void testExample1(final CdfContent content) throws Exception {
    final GlobalAttribute[] gatts = content.getGlobalAttributes();

    assertTrue(gatts.length == 1);

    final GlobalAttribute gatt0 = gatts[0];
    assertEquals("TITLE", gatt0.getName());

    assertArrayEquals(new String[]{"CDF title", "Author: CDF"},
      getEntryShapedValues(gatt0.getEntries()));

    final VariableAttribute[] vatts = content.getVariableAttributes();
    assertTrue(vatts.length == 2);
    assertEquals("FIELDNAM", vatts[0].getName());
    assertEquals("UNITS", vatts[1].getName());

    final Variable[] vars = content.getVariables();
    assertTrue(vars.length == 3);
    assertEquals("Time", vars[0].getName());
    assertEquals("Latitude", vars[1].getName());
    assertEquals("Image", vars[2].getName());
    assertTrue(vars[0].getSummary().matches("INT4 .* 0:\\[\\] T/"));
    assertTrue(vars[1].getSummary().matches("INT2 .* 1:\\[181\\] T/T"));
    assertTrue(vars[2].getSummary().matches("INT4 .* 2:\\[10,20\\] T/TT"));

    assertEquals("Hour/Minute", vatts[1].getEntry(vars[0]).getShapedValue());
    assertNull(vatts[1].getEntry(vars[1]));

    assertEquals(23, readShapedRecord(vars[0], 0, true));
    assertEquals(24, readShapedRecord(vars[0], 1, true));
    assertNull(readShapedRecord(vars[0], 2, true));
    assertArrayEquals((short[]) readShapedRecord(vars[1], 0, true),
      shortSequence(-90, 1, 181));
    assertArrayEquals((short[]) readShapedRecord(vars[1], 0, false),
      shortSequence(-90, 1, 181));
    assertNull(readShapedRecord(vars[1], 1, true));
    assertNull(readShapedRecord(vars[1], 2, false));
    assertArrayEquals((int[]) readShapedRecord(vars[2], 0, true),
      intSequence(0, 1, 200));
    assertArrayEquals((int[]) readShapedRecord(vars[2], 1, true),
      intSequence(200, 1, 200));
    assertArrayEquals((int[]) readShapedRecord(vars[2], 2, true),
      intSequence(400, 1, 200));
    final int[] sideways = (int[]) readShapedRecord(vars[2], 0, false);
    assertEquals(0, sideways[0]);
    assertEquals(20, sideways[1]);
    assertEquals(40, sideways[2]);
    assertEquals(1, sideways[10]);
    assertEquals(199, sideways[199]);
  }

  @Test
  public void testExample2() throws Exception {
    final File file = new File("example2.cdf");
    final CdfContent content = new CdfContent(new CdfReader(file));
    testExample2(content);
  }

  public void testExample2(final CdfContent content) throws Exception {
    final GlobalAttribute[] gatts = content.getGlobalAttributes();
    assertEquals(1, gatts.length);
    final GlobalAttribute gatt0 = gatts[0];
    assertEquals("TITLE", gatt0.getName());
    assertEquals("An example CDF (2).", ((String) gatt0.getEntries()[0].getShapedValue())
      .trim());

    final VariableAttribute[] vatts = content.getVariableAttributes();
    assertEquals(9, vatts.length);
    final VariableAttribute fnVatt = vatts[0];
    final VariableAttribute vminVatt = vatts[1];
    final VariableAttribute vmaxVatt = vatts[2];
    assertEquals("FIELDNAM", fnVatt.getName());
    assertEquals("VALIDMIN", vminVatt.getName());
    assertEquals("VALIDMAX", vmaxVatt.getName());

    final Variable[] vars = content.getVariables();
    assertEquals(4, vars.length);
    final Variable timeVar = vars[0];
    final Variable lonVar = vars[1];
    final Variable latVar = vars[2];
    final Variable tempVar = vars[3];
    assertEquals("Time", timeVar.getName());
    assertEquals("Longitude", lonVar.getName());
    assertEquals("Latitude", latVar.getName());
    assertEquals("Temperature", tempVar.getName());

    assertTrue(timeVar.getSummary().matches("INT4 .* 0:\\[\\] T/"));
    assertTrue(lonVar.getSummary().matches("REAL4 .* 1:\\[2\\] F/T"));
    assertTrue(latVar.getSummary().matches("REAL4 .* 1:\\[2\\] F/T"));
    assertTrue(tempVar.getSummary().matches("REAL4 .* 2:\\[2,2\\] T/TT"));
    assertEquals(24, timeVar.getRecordCount());
    assertEquals(24, tempVar.getRecordCount());
    assertEquals(1, lonVar.getRecordCount());
    assertEquals(1, latVar.getRecordCount());

    assertEquals("Time of observation", ((String) fnVatt.getEntry(timeVar).getShapedValue()).trim());
    assertEquals(0, vminVatt.getEntry(timeVar).getShapedValue());
    assertEquals(2359, vmaxVatt.getEntry(timeVar).getShapedValue());
    assertEquals(-180f, vminVatt.getEntry(lonVar).getShapedValue());
    assertEquals(180f, vmaxVatt.getEntry(lonVar).getShapedValue());

    assertEquals(0, readShapedRecord(timeVar, 0, true));
    assertEquals(2300, readShapedRecord(timeVar, 23, false));

    final float[] lonVal = new float[]{-165f, -150f};
    final float[] latVal = new float[]{40f, 30f};
    for (int irec = 0; irec < 24; irec++) {
      assertArrayEquals((float[]) readShapedRecord(lonVar, irec,
        true),
        lonVal, 0);
      assertArrayEquals((float[]) readShapedRecord(latVar, irec,
        false),
        latVal, 0);
    }
    assertArrayEquals((float[]) readShapedRecord(tempVar, 0, true),
      new float[]{20f, 21.7f, 19.2f, 20.7f}, 0);
    assertArrayEquals((float[]) readShapedRecord(tempVar, 23, true),
      new float[]{21f, 19.5f, 18.4f, 22f}, 0);
    assertArrayEquals((float[]) readShapedRecord(tempVar, 23, false),
      new float[]{21f, 18.4f, 19.5f, 22f}, 0);
  }

  @Test
  public void testTest() throws Exception {
    final File file = new File("test.cdf");
    final CdfContent content = new CdfContent(new CdfReader(file));
    testTest(content);
  }

  public void testTest(final CdfContent content) throws Exception {
    final GlobalAttribute[] gatts = content.getGlobalAttributes();
    assertEquals(5, gatts.length);
    assertEquals("Project", gatts[0].getName());
    final GlobalAttribute gatt1 = gatts[1];
    assertEquals("PI", gatt1.getName());
    assertArrayEquals(new String[]{null, null, null, "Ernie Els"},
      getEntryShapedValues(gatt1.getEntries()));
    final GlobalAttribute gatt2 = gatts[2];
    assertEquals("Test", gatt2.getName());
    final AttributeEntry[] tents = gatt2.getEntries();
    assertEquals(5.3432, tents[0].getShapedValue());
    assertNull(tents[1]);
    assertEquals(5.5f, tents[2].getShapedValue());
    assertArrayEquals((float[]) tents[3].getShapedValue(),
      new float[]{5.5f, 10.2f}, 0);
    assertArrayEquals((float[]) tents[3].getRawValue(),
      new float[]{5.5f, 10.2f}, 0);
    assertEquals(1, ((Byte) tents[4].getShapedValue()).byteValue());
    assertArrayEquals((byte[]) tents[5].getShapedValue(),
      new byte[]{(byte) 1, (byte) 2, (byte) 3});
    assertEquals(-32768, ((Short) tents[6].getShapedValue()).shortValue());
    assertArrayEquals((short[]) tents[7].getShapedValue(),
      new short[]{(short) 1, (short) 2});
    assertEquals(((Integer) tents[8].getShapedValue()).intValue(), 3);
    assertArrayEquals((int[]) tents[9].getShapedValue(),
      new int[]{4, 5});
    assertEquals("This is a string", tents[10].getShapedValue());
    assertEquals(4294967295L, ((Long) tents[11].getShapedValue()).longValue());
    assertArrayEquals((long[]) tents[12].getShapedValue(),
      new long[]{4294967295L, 2147483648L});
    assertEquals(65535, ((Integer) tents[13].getShapedValue()).intValue());
    assertArrayEquals((int[]) tents[14].getShapedValue(),
      new int[]{65535, 65534});
    assertEquals(255, ((Short) tents[15].getShapedValue()).shortValue());
    assertArrayEquals((short[]) tents[16].getShapedValue(),
      new short[]{255, 254});

    final EpochFormatter epf = new EpochFormatter();
    final GlobalAttribute gatt3 = gatts[3];
    assertEquals("TestDate", gatt3.getName());
    assertEquals("2002-04-25T00:00:00.000", epf
      .formatEpoch(((Double) gatt3.getEntries()[1].getShapedValue())));
    assertEquals("2008-02-04T06:08:10.012014016", epf
      .formatTimeTt2000(((Long) gatt3.getEntries()[2]
        .getShapedValue())));
    final double[] epDate = (double[]) gatts[4].getEntries()[0].getShapedValue();
    assertEquals("2004-05-13T15:08:11.022033044055", epf.formatEpoch16(epDate[0], epDate[1]));

    final Variable[] vars = content.getVariables();
    final Variable latVar = vars[0];
    assertEquals("Latitude", latVar.getName());
    assertArrayEquals(new byte[]{(byte) 1, (byte) 2, (byte) 3},
      (byte[]) readShapedRecord(latVar, 0, true));
    assertArrayEquals(new byte[]{(byte) 1, (byte) 2, (byte) 3},
      (byte[]) readShapedRecord(latVar, 100, true));

    final Variable lat1Var = vars[1];
    assertEquals("Latitude1", lat1Var.getName());
    assertArrayEquals(new short[]{(short) 100, (short) 128,
      (short) 255},
      (short[]) readShapedRecord(lat1Var, 2, true));

    final Variable longVar = vars[2];
    assertEquals("Longitude", longVar.getName());
    assertArrayEquals(new short[]{(short) 100, (short) 200,
      (short) 300},
      (short[]) readShapedRecord(longVar, 0, true));
    assertArrayEquals(new short[]{(short) -32767, (short) -32767,
      (short) -32767},
      (short[]) readShapedRecord(longVar, 1, true));

    final Variable nameVar = vars[8];
    assertEquals("Name", nameVar.getName());
    assertArrayEquals(new String[]{"123456789 ", "13579     "},
      (String[]) readShapedRecord(nameVar, 0, true));

    final Variable tempVar = vars[9];
    assertEquals("Temp", tempVar.getName());
    assertArrayEquals(new float[]{55.5f, -1e30f, 66.6f},
      (float[]) readShapedRecord(tempVar, 0, true), 0);
    assertArrayEquals(new float[]{-1e30f, -1e30f, -1e30f},
      (float[]) readShapedRecord(tempVar, 1, true), 0);

    final Variable epVar = vars[15];
    assertEquals("ep", epVar.getName());
    assertEquals("1999-03-05T05:06:07.100", epf
      .formatEpoch((Double) readShapedRecord(epVar, 0)));
    assertEquals("1999-03-05T05:06:07.100", epf
      .formatEpoch((Double) readShapedRecord(epVar, 0)));

    final Variable ep16Var = vars[16];
    assertEquals("ep16", ep16Var.getName());
    final double[] ep2 = (double[]) readShapedRecord(ep16Var, 1, true);
    assertEquals("2004-12-29T16:56:24.031411522634", epf.formatEpoch16(ep2[0], ep2[1]));

    final Variable ttVar = vars[18];
    assertEquals("tt2000", ttVar.getName());
    assertEquals("2015-06-30T23:59:58.123456789", epf.formatTimeTt2000((Long) readShapedRecord(ttVar, 0)));
    assertEquals("2015-06-30T23:59:60.123456789", epf.formatTimeTt2000((Long) readShapedRecord(ttVar, 2)));
    assertEquals("2015-07-01T00:00:00.123456789", epf.formatTimeTt2000((Long) readShapedRecord(ttVar, 3)));
  }

  private Object readShapedRecord(final Variable var, final int irec, final boolean rowMajor)
    throws IOException {
    return var.readShapedRecord(irec, rowMajor,
      var.createRawValueArray());
  }

  private Object readShapedRecord(final Variable var, final int irec)
    throws IOException {
    return readShapedRecord(var, irec, true);
  }

  private short[] shortSequence(final int start, final int step, final int count) {
    final short[] array = new short[count];
    for (int i = 0; i < count; i++) {
      array[i] = (short) (start + i * step);
    }
    return array;
  }

  private int[] intSequence(final int start, final int step, final int count) {
    final int[] array = new int[count];
    for (int i = 0; i < count; i++) {
      array[i] = start + i * step;
    }
    return array;
  }

  private static Object[] getEntryShapedValues(final AttributeEntry[] entries) {
    final int nent = entries.length;
    final Object[] vals = new Object[nent];
    for (int ie = 0; ie < nent; ie++) {
      final AttributeEntry entry = entries[ie];
      vals[ie] = entry == null ? null : entry.getShapedValue();
    }
    return vals;
  }

  @Test
  public void testLargeBuf() throws Exception {
    final File f = new File("example1.cdf");
    try (FileInputStream ifs = new FileInputStream(f)) {
      final Buf buf = Bufs.createLargeBuf(ifs, 1024, true, false);
      CdfContent content = new CdfContent(new CdfReader(buf));
      testExample1(content);
    }
  }

  public ByteBuffer toByteBuffer(final byte[] b, final int len) {
    return ByteBuffer.wrap(cp(b, len));
  }

  public ByteBuffer toByteBuffer(final byte[] b) {
    return ByteBuffer.wrap(cp(b));
  }

  public byte[] cp(final byte[] b, final int len) {
    final byte[] newArray = new byte[len];
    for (int i = 0; i < len; i++) {
      newArray[i] = b[i];
    }
    return newArray;
  }

  public byte[] cp(final byte[] b) {
    return cp(b, b.length);
  }
}
