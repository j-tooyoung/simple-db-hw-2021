package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int[] bucketArray;
    private double width;
    private int tupleCnts;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        if (min > max) {
            return;
        }
        bucketArray = new int[buckets];
        // some code goes here
        this.min = min;
        this.max = max;
        this.width = Math.max(1, (max - min + 1.0) / buckets);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v > max || v < min) {
            return;
        }
        // some code goes here
        int idx = getIndex(v);
        bucketArray[idx]++;
        tupleCnts++;
    }

    private int getIndex(int v) {
        return (int) ((v - this.min) / width);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int cnt = 0;
        int idx = getIndex(v);
        switch (op) {
            case GREATER_THAN -> {
                if (v < min) {
                    return 1.0;
                }
                if (v > max) {
                    return 0.0;
                }
                int maxIdx = getIndex(max);
                for (int i = (idx + 1); i <= maxIdx; i += 1) {
                    cnt += bucketArray[i];
                }
                return cnt * 1.0 / tupleCnts;
            }
            case GREATER_THAN_OR_EQ -> {
                return 1.0 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            }
            case LESS_THAN -> {
                if (v > max) {
                    return 1.0;
                }
                if (v < min) {
                    return 0.0;
                }
                for (int i = 0; i < idx; i += 1) {
                    cnt += bucketArray[i];
                }
                return cnt * 1.0 / tupleCnts;
            }
            case LESS_THAN_OR_EQ -> {
                return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            }
            case EQUALS -> {
                return bucketArray[idx] * 1.0 / tupleCnts;
            }
            case NOT_EQUALS -> {
                return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
            }
        }
        // some code goes here
        return -1.0;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        int ans = 0;
        for (int num : bucketArray) {
            ans += num;
        }
        return ans * 1.0 / tupleCnts;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
