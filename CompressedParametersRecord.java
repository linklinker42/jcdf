package cdf.record;

import java.io.IOException;

/**
 * Field data for CDF record of type Compressed Parameters Record.
 *
 * @author   Mark Taylor
 * @since    19 Jun 2013
 */
public class CompressedParametersRecord extends Record {

    public final int cType_;
    public final int rfuA_;
    public final int pCount_;
    public final int[] cParms_;

    /**
     * Constructor.
     *
     * @param  plan   basic record information
     */
    public CompressedParametersRecord( RecordPlan plan ) throws IOException {
        super( plan, "CPR", 11 );
        Buf buf = plan.getBuf();
        Pointer ptr = plan.createContentPointer();
        cType_ = buf.readInt( ptr );
        rfuA_ = checkIntValue( buf.readInt( ptr ), 0 );
        pCount_ = buf.readInt( ptr );
        cParms_ = readIntArray( buf, ptr, pCount_ );
        checkEndRecord( ptr );
    }
}
