package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op aggregationOp;

    private GbHandler hanlder;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.aggregationOp = what;
        if (what == Op.COUNT) {
            hanlder = new CountHandler();
        } else if (what == Op.SUM) {
            hanlder = new SumHandler();
        } else {
            throw new UnsupportedOperationException("operator it don't meet require " + what);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field =  tup.getField(afield);
        Field gbField = tup.getField(gbfield);
        String key = gbField.toString();
        hanlder.handle(key, field);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
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
            Type[] typeAr = new Type[]{Type.STRING_TYPE, Type.INT_TYPE};
            String[] fieldAr = new String[]{
                    "groupVal", "aggregateVal"};
            td = new TupleDesc(typeAr, fieldAr);
            for (Map.Entry<String, Integer> entry : entrySet) {
                Tuple e = new Tuple(td);
                e.setField(0, new StringField(entry.getKey(), 100));
                e.setField(1, new IntField(entry.getValue()));
                tuples.add(e);
            }
        }

        return new TupleIterator(td, tuples);
    }
}
