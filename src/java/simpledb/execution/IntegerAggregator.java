package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op aggregationOp;

    private GbHandler hanlder;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.aggregationOp = what;
        switch (what) {
            case MIN -> hanlder = new MinHandler();
            case MAX -> hanlder = new MaxHandler();
            case COUNT -> hanlder = new CountHandler();
            case AVG -> hanlder = new AvgHandler();
            case SUM -> hanlder = new SumHandler();
            case SUM_COUNT -> hanlder = new SumCountHandler();
            case SC_AVG -> hanlder = new ScAvgHandler();
            default -> throw new UnsupportedOperationException("operator it don't meet require " + what);
        }
    }

    private static final String NO_GROUPING_KEY = "NO_GROUPING_KEY";

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field = tup.getField(afield);
        Field gbField = tup.getField(gbfield);
        String key = gbField == null ? NO_GROUPING_KEY : gbField.toString();
        hanlder.handle(key, field);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc td;

        List<Tuple> tuples = new ArrayList<>();
        Set<Map.Entry<String, Integer>> entrySet = hanlder.getGbResult().entrySet();
        if (gbfield == NO_GROUPING) {
            Type[] typeAr = new Type[]{Type.INT_TYPE};
            String[] fieldAr = new String[1];
            fieldAr[0] = "aggregateVal";
            td = new TupleDesc(typeAr, fieldAr);
            for (Map.Entry<String, Integer> entry : entrySet) {
                Tuple e = new Tuple(td);
                e.setField(0, new IntField(entry.getValue()));
                tuples.add(e);
            }
        } else {
            Type[] typeAr = new Type[]{Type.INT_TYPE, Type.INT_TYPE};
            String[] fieldAr = new String[]{
                    "groupVal", "aggregateVal"};
            td = new TupleDesc(typeAr, fieldAr);
            for (Map.Entry<String, Integer> entry : entrySet) {
                Tuple e = new Tuple(td);
                e.setField(0, new IntField(Integer.parseInt(entry.getKey())));
                e.setField(1, new IntField(entry.getValue()));
                tuples.add(e);
            }
        }

        return new TupleIterator(td, tuples);
    }
}
