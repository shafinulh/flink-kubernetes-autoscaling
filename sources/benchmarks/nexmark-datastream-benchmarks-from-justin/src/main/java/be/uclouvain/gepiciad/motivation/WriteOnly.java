package be.uclouvain.gepiciad.motivation;

import be.uclouvain.gepiciad.sources.Event;
import be.uclouvain.gepiciad.sources.SequenceGeneratorSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

public class WriteOnly {

    public static void main(String[] args) throws Exception {
        ParameterTool pt = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(pt.getInt("parallelism", 1));
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        KeyedStream<Event, Integer> keyedStream = env.addSource(SequenceGeneratorSource.createSourceFromParameters(pt))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(SequenceGeneratorSource.createTimestampExtractor(pt))
                .keyBy(Event::getKey);


        keyedStream.map(new ValueStateMapper())
                .name("Mapper")
                .uid("Mapper")
                .disableChaining()
                .addSink(new DiscardingSink<>())
                .name("Sink")
                .uid("Sink");
        env.execute("WriteOnly microbenchmark.");
    }

    private static class ValueStateMapper extends RichMapFunction<Event, String> {

        private static final long serialVersionUID = 1L;

        private transient ValueState<String> valueState;

        @Override
        public void open(Configuration parameters) {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            valueState =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "valueState" + index, StringSerializer.INSTANCE));
        }

        @Override
        public String map(Event event) throws Exception {
            valueState.update(event.getPayload());
            return event.getPayload();
        }
    }
}
