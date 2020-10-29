package org.sense.flink.examples.table.udf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.sense.flink.examples.table.util.TaxiRide;
import org.sense.flink.util.DataRateListener;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public class TaxiRideSourceParallel extends RichParallelSourceFunction<TaxiRide> {

    private final int maxDelayMsecs;
    private final int watermarkDelayMSecs;

    private final String dataFilePath;
    private final int servingSpeed;
    private DataRateListener dataRateListener;
    private boolean running;
    private transient BufferedReader reader;
    private transient InputStream gzipStream;

    public TaxiRideSourceParallel(String dataFilePath) {
        this(dataFilePath, 0, 1);
    }

    public TaxiRideSourceParallel(String dataFilePath, int servingSpeedFactor) {
        this(dataFilePath, 0, servingSpeedFactor);
    }

    public TaxiRideSourceParallel(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
        if (maxEventDelaySecs < 0) {
            throw new IllegalArgumentException("Max event delay must be positive");
        }
        this.running = true;
        this.dataFilePath = dataFilePath;
        this.maxDelayMsecs = maxEventDelaySecs * 1000;
        this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.dataRateListener = new DataRateListener();
        this.dataRateListener.start();
    }

    @Override
    public void run(SourceContext<TaxiRide> sourceContext) throws Exception {
        while (running) {
            generateTaxiRideArray(sourceContext);
        }

        this.reader.close();
        this.reader = null;
        this.gzipStream.close();
        this.gzipStream = null;
    }

    private void generateTaxiRideArray(SourceContext<TaxiRide> sourceContext) throws Exception {
        gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8));
        String line;
        TaxiRide taxiRide;
        long startTime;
        while (reader.ready() && (line = reader.readLine()) != null) {
            startTime = System.nanoTime();
            taxiRide = TaxiRide.fromString(line);

            sourceContext.collectWithTimestamp(taxiRide, getEventTime(taxiRide));

            // sleep in nanoseconds to have a reproducible data rate for the data source
            this.dataRateListener.busySleep(startTime);
        }
    }

    public long getEventTime(TaxiRide ride) {
        return ride.getEventTime();
    }

    @Override
    public void cancel() {
        try {
            this.running = false;
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.gzipStream != null) {
                this.gzipStream.close();
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.gzipStream = null;
        }
    }
}

