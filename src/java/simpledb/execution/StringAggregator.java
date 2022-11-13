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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> groups;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if(what != Op.COUNT) throw new IllegalArgumentException();

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groups = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // 1. get gbfield, agfield
        Field gbf = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        Field agf = tup.getField(this.afield);

        // 2. add to groups
        int v = this.groups.getOrDefault(gbf, 0);
        this.groups.put(gbf, v + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        TupleDesc td;
        if(this.gbfield == NO_GROUPING)
            td = new TupleDesc(new Type []{Type.INT_TYPE});
        else 
            td = new TupleDesc(new Type []{this.gbfieldtype, Type.INT_TYPE});

        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : this.groups.entrySet()) {

            Field f = entry.getKey();
            int val = entry.getValue();

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
