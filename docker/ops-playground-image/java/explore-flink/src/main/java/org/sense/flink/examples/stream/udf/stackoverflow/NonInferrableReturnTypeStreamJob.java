package org.sense.flink.examples.stream.udf.stackoverflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sense.flink.examples.stream.valencia.ValenciaSinkFunction;

import java.util.ArrayList;
import java.util.List;

public class NonInferrableReturnTypeStreamJob {

    private final List<AbstractDataModel> abstractDataModelList;
    private final ValenciaSinkFunction sink;

    public NonInferrableReturnTypeStreamJob() {
        this.abstractDataModelList = new ArrayList<AbstractDataModel>();
        this.abstractDataModelList.add(new ConcreteModel("a", "1"));
        this.abstractDataModelList.add(new ConcreteModel("a", "2"));
        this.sink = new ValenciaSinkFunction();
    }

    public NonInferrableReturnTypeStreamJob(List<AbstractDataModel> abstractDataModelList, ValenciaSinkFunction sink) {
        this.abstractDataModelList = abstractDataModelList;
        this.sink = sink;
    }

    public static void main(String[] args) throws Exception {
        NonInferrableReturnTypeStreamJob concreteModelTest = new NonInferrableReturnTypeStreamJob();
        concreteModelTest.execute();
    }

    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromCollection(this.abstractDataModelList)
                .map(new MyMapFunctionNonInferrableReturnType())
                .returns(TypeInformation.of(String.class))
                .addSink(sink);

        env.execute();
    }
}
