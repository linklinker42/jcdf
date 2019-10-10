package uk.ac.bristol.star.cdf.record;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import uk.ac.bristol.star.cdf.CdfFormatException;

/**
 * Turns bytes in a buffer into typed and populated CDF records.
 *
 * @author Mark Taylor
 * @since 18 Jun 2013
 */
public class RecordFactory {

  private static final Logger LOGGER = Logger.getLogger(RecordFactory.class.getName());
  private final Map<Integer, TypedRecordFactory> factoryMap_;

  /**
   * Constructor.
   *
   * @param nameLeng number of bytes in variable and attribute names;
   *                 appears to be 64 for pre-v3 and 256 for v3
   */
  public RecordFactory(final int nameLeng) {
    factoryMap_ = createFactoryMap(nameLeng);
  }

  /**
   * Creates a Record object from a given position in a buffer.
   * The returned object will be an instance of one of the
   * Record subclasses as appropriate for its type.
   *
   * @param buf    byte buffer
   * @param offset start of record in buf
   *
   * @return record
   *
   * @throws java.io.IOException
   */
  public Record createRecord(final Buf buf, final long offset) throws IOException {
    final Pointer ptr = new Pointer(offset);
    final long recSize = buf.readOffset(ptr);
    final int recType = buf.readInt(ptr);
    final RecordPlan plan = new RecordPlan(offset, recSize, recType, buf);
    final TypedRecordFactory tfact = factoryMap_.get(recType);
    if (tfact == null) {
      throw new CdfFormatException("Unknown record type " + recType);
    } else {
      final Record rec = tfact.createRecord(plan);
      final String msg = new StringBuffer()
        .append("CDF Record:\t")
        .append("0x")
        .append(Long.toHexString(offset))
        .append("\t+")
        .append(recSize)
        .append("\t")
        .append(rec.getRecordTypeAbbreviation())
        .toString();
      LOGGER.config(msg);
      return rec;
    }
  }

  /**
   * Creates a Record object with a known type from a given position in
   * a buffer. This simply calls the untyped <code>getRecord</code>
   * method, and attempts to cast the result, throwing a
   * CdfFormatException if it has the wrong type.
   *
   * @param <R>
   * @param buf    byte buffer
   * @param offset start of record in buf
   * @param clazz  record class asserted for the result
   *
   * @return record
   *
   * @throws CdfFormatException if the record found there turns out
   *                            not to be of type <code>clazz</code>
   */
  public <R extends Record> R createRecord(final Buf buf, final long offset,
    final Class<R> clazz)
    throws IOException {
    final Record rec = createRecord(buf, offset);
    if (clazz.isInstance(rec)) {
      return clazz.cast(rec);
    } else {
      final String msg = new StringBuffer()
        .append("Unexpected record type at ")
        .append("0x")
        .append(Long.toHexString(offset))
        .append("; got ")
        .append(rec.getClass().getName())
        .append(" not ")
        .append(clazz.getName())
        .toString();
      throw new CdfFormatException(msg);
    }
  }

  /**
   * Sets up a mapping from CDF RecordType codes to factories for the
   * record types in question.
   *
   * @return map of record type to record factory
   */
  private static Map<Integer, TypedRecordFactory> createFactoryMap(final int nameLeng) {
    final Map<Integer, TypedRecordFactory> map = new HashMap<>();
    map.put(1, (TypedRecordFactory) CdfDescriptorRecord::new);
    map.put(2, (TypedRecordFactory) GlobalDescriptorRecord::new);
    map.put(4, (TypedRecordFactory) (final RecordPlan plan) -> new AttributeDescriptorRecord(plan, nameLeng));
    map.put(5, (TypedRecordFactory) AttributeEntryDescriptorRecord.GrVariant::new);
    map.put(9, (TypedRecordFactory) AttributeEntryDescriptorRecord.ZVariant::new);
    map.put(3, (TypedRecordFactory) (final RecordPlan plan) -> new VariableDescriptorRecord.RVariant(plan, nameLeng));
    map.put(8, (TypedRecordFactory) (final RecordPlan plan) -> new VariableDescriptorRecord.ZVariant(plan, nameLeng));
    map.put(6, (TypedRecordFactory) VariableIndexRecord::new);
    map.put(7, (TypedRecordFactory) VariableValuesRecord::new);
    map.put(10, (TypedRecordFactory) CompressedCdfRecord::new);
    map.put(11, (TypedRecordFactory) CompressedParametersRecord::new);
    map.put(12, (TypedRecordFactory) SparsenessParametersRecord::new);
    map.put(13, (TypedRecordFactory) CompressedVariableValuesRecord::new);
    map.put(-1, (TypedRecordFactory) UnusedInternalRecord::new);
    final int[] recTypes = new int[map.size()];
    int irt = 0;
    for (final int recType : map.keySet()) {
      recTypes[irt++] = recType;
    }
    Arrays.sort(recTypes);
    assert Arrays.equals(recTypes, new int[]{-1, 1, 2, 3, 4, 5, 6, 7,
      8, 9, 10, 11, 12, 13});
    return Collections.unmodifiableMap(map);
  }

  /**
   * Object which can generate a particular record type from a plan.
   */
  private static interface TypedRecordFactory<R extends Record> {

    /**
     * Creates a record from bytes.
     *
     * @param plan basic record information
     *
     * @return record
     */
    R createRecord(final RecordPlan plan) throws IOException;
  }
}
