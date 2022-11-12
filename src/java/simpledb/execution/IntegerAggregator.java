package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, ArrayList<Integer>> groups;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groups = new HashMap<Field, ArrayList<Integer>>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // 1. get gbfield, agfield
        Field gbf = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        Field agf = tup.getField(this.afield);

        // 2. add to groups
        ArrayList<Integer> arr = this.groups.get(gbf);
        if (arr == null) {
            arr = new ArrayList<Integer>();
            this.groups.put(gbf, arr);
        }

        if (agf.getType() == Type.INT_TYPE)
            arr.add(((IntField)(agf)).getValue());
        else
            arr.add(0);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        TupleDesc td;
        if(this.gbfield == NO_GROUPING)
            td = new TupleDesc(new Type []{Type.INT_TYPE});
        else 
            td = new TupleDesc(new Type []{this.gbfieldtype, Type.INT_TYPE});

        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, ArrayList<Integer>> entry : this.groups.entrySet()) {

            Field f = entry.getKey();
            ArrayList<Integer> arr = entry.getValue();

            int val = 0;

            switch (this.what) {
                case AVG:
                    for (int i : arr) val += i;
                    val /= arr.size();
                    break;
                case MAX:
                    int max = Integer.MIN_VALUE;
                    for (int i : arr)
                        max = Math.max(max, i);
                    val = max;
                    break;
                case MIN:
                    int min = Integer.MAX_VALUE;
                    for (int i : arr)
                        min = Math.min(min, i);
                    val = min;
                    break;
                case SUM:
                    for (int i : arr) val += i;
                    break;
                case COUNT:
                    val = arr.size();
                    break;
                case SC_AVG:
                    break;
                case SUM_COUNT:
                    break;
            }

            Tuple tup = new Tuple(td);
            if(this.gbfield == NO_GROUPING) {
                tup.setField(0, new IntField(val));
            }
            else {
                tup.setField(0, f);
                tup.setField(1, new IntField(val));
            }

            tuples.add(tup);
        }

        return new TupleIterator(td, tuples);
    }

}
