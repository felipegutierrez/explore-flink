package org.sense.flink.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class WordsSource extends RichSourceFunction<String> {
    private final long maxCount;
    private final String inputUrlFile;
    private final String inputTextFile;
    private boolean running;

    public WordsSource() {
        this(-1);
    }

    public WordsSource(long maxCount) {
        this.running = true;
        this.maxCount = maxCount;
        this.inputUrlFile = "http://erdani.com/tdpl/hamlet.txt";
        this.inputTextFile = "/hamlet.txt";
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        try {
            long count = 0;
            while (running) {
                count++;

                // read file lines
                readFileLine(sourceContext, inputTextFile);

                // decide when to stop generate data
                if (this.maxCount != -1 && count >= this.maxCount) {
                    this.running = false;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    private void readFileLine(SourceContext<String> sourceContext, String inputTextFile) throws IOException, InterruptedException {
        InputStream is = WordsSource.class.getResourceAsStream(inputTextFile);
        if (is == null) {
            System.err.println("Could not load file [" + inputTextFile + "].");
        } else {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // process the line.
                    sourceContext.collect(line);
                    Thread.sleep(1000);
                }
            }
        }
    }
}
