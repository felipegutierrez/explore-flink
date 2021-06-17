package org.sense.flink.examples.stream.edgent;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class ExceptionSimulatorProcess extends ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>
        implements CheckpointedFunction {

    public static final SimpleDateFormat sdfMillis = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    public static final String POISON_TRANSACTION_ID = "66666666";
    private static final Logger LOG = LoggerFactory.getLogger(ExceptionSimulatorProcess.class);
    final OutputTag<Long> outputTag = new OutputTag<Long>("side-output") {
    };

    private final long currentTimeMillisBehind;
    private final long referenceTimeMillisAhead;
    private long currentTimeMillis;
    private transient ListState<Long> restartsState;
    private Long restartsLocal;

    public ExceptionSimulatorProcess(long currentTimeMillisBehind, long millisAhead) {
        this.currentTimeMillisBehind = currentTimeMillisBehind;
        this.currentTimeMillis = System.currentTimeMillis() - this.currentTimeMillisBehind;
        this.referenceTimeMillisAhead = this.currentTimeMillis + millisAhead;
        this.restartsLocal = 0L;
    }

    @Override
    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        this.currentTimeMillis = System.currentTimeMillis() - currentTimeMillisBehind;

        // If current time is less than the reference time ahead AND we have the poison auction an exception will throw
        if (this.currentTimeMillis < this.referenceTimeMillisAhead && POISON_TRANSACTION_ID.equals(value.f0)) {

            LOG.error("This exception will trigger until the reference time [{}] reaches the trigger time [{}]",
                    sdfMillis.format(new Date(this.currentTimeMillis)),
                    sdfMillis.format(new Date(this.referenceTimeMillisAhead)));

            throw new SimulatedException("Transaction ID: " + value.f0 + " not allowed. This is a simple exception for testing purposes.");
        }
        out.collect(value);

        // counts the restarts
        if (restartsState != null) {
            List<Long> restoreList = Lists.newArrayList(restartsState.get());
            Long attemptsRestart = 0L;
            if (restoreList != null && !restoreList.isEmpty()) {
                attemptsRestart = Collections.max(restoreList);
                if (restartsLocal < attemptsRestart) {
                    restartsLocal = attemptsRestart;
                    ctx.output(outputTag, Long.valueOf(attemptsRestart));
                }
            }
            LOG.info("Attempts restart: " + attemptsRestart);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        restartsState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Long>("restarts", Long.class));

        if (context.isRestored()) {
            List<Long> restoreList = Lists.newArrayList(restartsState.get());
            if (restoreList == null || restoreList.isEmpty()) {
                restartsState.add(1L);
                LOG.info("restarts: 1");
            } else {
                Long max = Collections.max(restoreList);
                LOG.info("restarts: " + max);
                restartsState.add(max + 1);
            }
        } else {
            LOG.info("restarts: never restored");
        }
    }
}

class SimulatedException extends Exception {
    public SimulatedException() {
        super("This is a simulated exception to test checkpoint trigger for auctions.");
    }

    public SimulatedException(String message) {
        super(message);
    }

    public SimulatedException(Throwable cause) {
        super(cause);
    }

    public SimulatedException(String message, Throwable cause) {
        super(message, cause);
    }
}

