package org.sense.flink.examples.stream.udf.stackoverflow;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.sense.flink.examples.stream.valencia.ValenciaSinkFunction;

import java.util.ArrayList;
import java.util.List;

public class ConcreteModelStreamJob {

    private final List<AbstractDataModel> abstractDataModelList;
    private final SinkFunction sink;

    public ConcreteModelStreamJob() {
        this.abstractDataModelList = new ArrayList<AbstractDataModel>();
        this.abstractDataModelList.add(new ConcreteModel("a", "1"));
        this.abstractDataModelList.add(new ConcreteModel("a", "2"));
        this.sink = new ValenciaSinkFunction();
    }

    public ConcreteModelStreamJob(List<AbstractDataModel> abstractDataModelList, SinkFunction sink) {
        this.abstractDataModelList = abstractDataModelList;
        this.sink = sink;
    }

    public static void main(String[] args) throws Exception {
        ConcreteModelStreamJob concreteModelTest = new ConcreteModelStreamJob();
        concreteModelTest.execute();
    }

    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromCollection(this.abstractDataModelList)
                .map(new MyMapFunction())
                .addSink(sink);

        env.execute();
    }
}
