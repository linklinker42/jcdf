package uk.ac.bristol.star.cdf.record;

import java.io.IOException;

/**
 * Field data for CDF record of type Attribute Descriptor Record.
 *
 * @author Mark Taylor
 * @since 19 Jun 2013
 */
public class AttributeDescriptorRecord extends Record {

  @CdfField
  @OffsetField
  public final long adrNext;
  @CdfField
  @OffsetField
  public final long agrEdrHead;
  @CdfField
  public final int scope;
  @CdfField
  public final int num;
  @CdfField
  public final int nGrEntries;
  @CdfField
  public final int maxGrEntry;
  @CdfField
  public final int rfuA;
  @CdfField
  @OffsetField
  public final long azEdrHead;
  @CdfField
  public final int nZEntries;
  @CdfField
  public final int maxZEntry;
  @CdfField
  public final int rfuE;
  @CdfField
  public final String name;

  /**
   * Constructor.
   *
   * @param plan     basic record info
   * @param nameLeng number of characters used for attribute names
   *
   * @throws java.io.IOException
   */
  public AttributeDescriptorRecord(final RecordPlan plan, final int nameLeng)
    throws IOException {
    super(plan, "ADR", 4);
    final Buf buf = plan.getBuf();
    final Pointer ptr = plan.createContentPointer();
    this.adrNext = buf.readOffset(ptr);
    this.agrEdrHead = buf.readOffset(ptr);
    this.scope = buf.readInt(ptr);
    this.num = buf.readInt(ptr);
    this.nGrEntries = buf.readInt(ptr);
    this.maxGrEntry = buf.readInt(ptr);
    this.rfuA = checkIntValue(buf.readInt(ptr), 0);
    this.azEdrHead = buf.readOffset(ptr);
    this.nZEntries = buf.readInt(ptr);
    this.maxZEntry = buf.readInt(ptr);
    this.rfuE = checkIntValue(buf.readInt(ptr), -1);
    this.name = buf.readAsciiString(ptr, nameLeng);
    checkEndRecord(ptr);
  }
}
