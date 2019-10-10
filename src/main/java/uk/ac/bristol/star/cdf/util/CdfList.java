package uk.ac.bristol.star.cdf.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import uk.ac.bristol.star.cdf.AttributeEntry;
import uk.ac.bristol.star.cdf.CdfContent;
import uk.ac.bristol.star.cdf.CdfReader;
import uk.ac.bristol.star.cdf.DataType;
import uk.ac.bristol.star.cdf.GlobalAttribute;
import uk.ac.bristol.star.cdf.Variable;
import uk.ac.bristol.star.cdf.VariableAttribute;

/**
 * Utility to describe a CDF file, optionally with record data.
 * Intended to be used from the commandline via the <code>main</code> method.
 * The output format is somewhat reminiscent of the <code>cdfdump</code>
 * command in the CDF distribution.
 *
 * @author Mark Taylor
 * @since 21 Jun 2013
 */
public class CdfList {

  private static final String[] NOVARY_MARKS = {"{ ", " }"};
  private static final String[] VIRTUAL_MARKS = {"[ ", " ]"};
  private static final String[] REAL_MARKS = {"  ", ""};
  private final CdfContent cdf_;
  private final PrintStream out_;
  private final boolean writeData_;

  /**
   * Constructor.
   *
   * @param cdf       CDF content
   * @param out       output stream for listing
   * @param writeData true if data values as well as metadata are to
   *                  be written
   */
  public CdfList(final CdfContent cdf, final PrintStream out, final boolean writeData) {
    cdf_ = cdf;
    out_ = out;
    writeData_ = writeData;
  }

  /**
   * Does the work, writing output.
   *
   * @throws java.io.IOException
   */
  public void run() throws IOException {

    // Read the CDF.
    final GlobalAttribute[] gAtts = cdf_.getGlobalAttributes();
    final VariableAttribute[] vAtts = cdf_.getVariableAttributes();
    final Variable[] vars = cdf_.getVariables();

    // Write global attribute information.
    header("Global Attributes");
    for (int iga = 0; iga < gAtts.length; iga++) {
      final GlobalAttribute gAtt = gAtts[iga];
      out_.println("    " + gAtt.getName());
      final AttributeEntry[] entries = gAtt.getEntries();
      for (int ie = 0; ie < entries.length; ie++) {
        out_.println("        " + entries[ie]);
      }
    }

    // Write variable information.
    for (int iv = 0; iv < vars.length; iv++) {
      out_.println();
      final Variable var = vars[iv];
      header("Variable " + var.getNum() + ": " + var.getName()
        + "  ---  " + var.getSummary());
      for (int ia = 0; ia < vAtts.length; ia++) {
        final VariableAttribute vAtt = vAtts[ia];
        final AttributeEntry entry = vAtt.getEntry(var);
        if (entry != null) {
          out_.println("    " + vAtt.getName() + ":\t" + entry);
        }
      }

      // Optionally write variable data as well.
      if (writeData_) {
        final DataType dataType = var.getDataType();
        final Object abuf = var.createRawValueArray();
        final boolean isVar = var.getRecordVariance();
        final int nrec = var.getRecordCount();
        final int nrdigit = Integer.toString(nrec).length();
        for (int ir = 0; ir < nrec; ir++) {
          var.readRawRecord(ir, abuf);
          final String[] marks;
          if (!isVar) {
            marks = NOVARY_MARKS;
          } else if (!var.hasRecord(ir)) {
            marks = VIRTUAL_MARKS;
          } else {
            marks = REAL_MARKS;
          }
          final String sir = Integer.toString(ir);
          final StringBuffer sbuf = new StringBuffer()
            .append(marks[0])
            .append(CdfDump.spaces(nrdigit - sir.length()))
            .append(sir)
            .append(':')
            .append('\t')
            .append(formatValues(abuf, dataType))
            .append(marks[1]);
          out_.println(sbuf.toString());
        }
      }
    }
  }

  /**
   * Applies string formatting to a value of a given data type.
   *
   * @param abuf     array buffer containing data
   * @param dataType data type for data
   *
   * @return string representation of value
   */
  private String formatValues(final Object abuf, final DataType dataType) {
    final StringBuffer sbuf = new StringBuffer();
    if (abuf == null) {
    } else if (abuf.getClass().isArray()) {
      final int groupSize = dataType.getGroupSize();
      final int len = Array.getLength(abuf);
      for (int i = 0; i < len; i += groupSize) {
        if (i > 0) {
          sbuf.append(", ");
        }
        sbuf.append(dataType.formatArrayValue(abuf, i));
      }
    } else {
      sbuf.append(dataType.formatScalarValue(abuf));
    }
    return sbuf.toString();
  }

  /**
   * Writes a header to the output listing.
   *
   * @param txt header text
   */
  private void header(final String txt) {
    out_.println(txt);
    final StringBuffer sbuf = new StringBuffer(txt.length());
    for (int i = 0; i < txt.length(); i++) {
      sbuf.append('-');
    }
    out_.println(sbuf.toString());
  }

  /**
   * Does the work for the command line tool, handling arguments.
   * Sucess is indicated by the return value.
   *
   * @param args command-line arguments
   *
   * @return 0 for success, non-zero for failure
   *
   * @throws java.io.IOException
   */
  public static int runMain(final String[] args) throws IOException {

    // Usage string.
    final String usage = new StringBuffer()
      .append("\n   Usage: ")
      .append(CdfList.class.getName())
      .append(" [-help]")
      .append(" [-verbose]")
      .append(" [-data]")
      .append(" <cdf-file>")
      .append("\n")
      .toString();

    // Process arguments.
    final List<String> argList = new ArrayList<String>(Arrays.asList(args));
    File file = null;
    boolean writeData = false;
    int verb = 0;
    for (Iterator<String> it = argList.iterator(); it.hasNext();) {
      final String arg = it.next();
      if (arg.startsWith("-h")) {
        it.remove();
        System.out.println(usage);
        return 0;
      } else if (arg.equals("-verbose") || arg.equals("-v")) {
        it.remove();
        verb++;
      } else if (arg.equals("+verbose") || arg.equals("+v")) {
        it.remove();
        verb--;
      } else if (arg.equals("-data")) {
        it.remove();
        writeData = true;
      } else if (file == null) {
        it.remove();
        file = new File(arg);
      }
    }

    // Validate arguments.
    if (!argList.isEmpty()) {
      System.err.println("Unused args: " + argList);
      System.err.println(usage);
      return 1;
    }
    if (file == null) {
      System.err.println(usage);
      return 1;
    }

    // Configure and run.
    LogUtil.setVerbosity(verb);
    new CdfList(new CdfContent(new CdfReader(file)),
      System.out, writeData).run();
    return 0;
  }

  /**
   * Main method. Use -help for arguments.
   *
   * @param args
   *
   * @throws java.io.IOException
   */
  public static void main(final String[] args) throws IOException {
    final int status = runMain(args);
    if (status != 0) {
      System.exit(status);
    }
  }
}
