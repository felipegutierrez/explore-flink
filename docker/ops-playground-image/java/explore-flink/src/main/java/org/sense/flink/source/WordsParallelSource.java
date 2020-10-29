package org.sense.flink.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class WordsParallelSource implements ParallelSourceFunction<String> {

    private final long maxCount;
    private final String inputTextFile;
    private boolean running;

    public WordsParallelSource() {
        this(-1);
    }

    public WordsParallelSource(long maxCount) {
        this("/hamlet.txt", maxCount);
    }

    public WordsParallelSource(String filePath, long maxCount) {
        this.running = true;
        this.maxCount = maxCount;
        this.inputTextFile = filePath;
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
                    sourceContext.collect(line);
                }
            }
        }
    }
}
