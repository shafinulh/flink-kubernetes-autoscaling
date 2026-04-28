package be.uclouvain.gepiciad.nexmark;
import be.uclouvain.gepiciad.sources.AuctionSourceFunction;
import be.uclouvain.gepiciad.sources.BidSourceFunction;
import be.uclouvain.gepiciad.sources.PersonSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;

public class Query3 {

    private static final Logger logger = LoggerFactory.getLogger(Query3.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable latency tracking
        // env.getConfig().setLatencyTrackingInterval(5000);

        env.disableOperatorChaining();

        final int auctionSrcRate = params.getInt("auction-srcRate", 20000);

        final int personSrcRate = params.getInt("person-srcRate", 10000);

        DataStream<Auction> auctions = env.addSource(new AuctionSourceFunction(auctionSrcRate))
                .name("Custom Source: Auctions")
                .setParallelism(params.getInt("p-auction-source", 1))
                .filter(new FilterFunction<Auction>() {
                    @Override
                    public boolean filter(Auction auction) throws Exception {
                        return auction.category == 10;
                    }
                });

        DataStream<Person> persons = env.addSource(new PersonSourceFunction(personSrcRate))
                .name("Custom Source: Persons")
                .setParallelism(params.getInt("p-person-source", 1))
                .filter(new FilterFunction<Person>() {
                    @Override
                    public boolean filter(Person person) throws Exception {
                        return (person.state.equals("OR") || person.state.equals("ID") || person.state.equals("CA"));
                    }
                })
                .setParallelism(params.getInt("p-person-source", 1));

        // SELECT Istream(P.name, P.city, P.state, A.id)
        // FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
        // WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA')

        KeyedStream<Auction, Long> keyedAuctions =
                auctions.keyBy(new KeySelector<Auction, Long>() {
                    @Override
                    public Long getKey(Auction auction) throws Exception {
                        return auction.seller;
                    }
                });

        KeyedStream<Person, Long> keyedPersons =
                persons.keyBy(new KeySelector<Person, Long>() {
                    @Override
                    public Long getKey(Person person) throws Exception {
                        return person.id;
                    }
                });

        DataStream<Tuple4<String, String, String, Long>> joined = keyedAuctions.connect(keyedPersons)
                .flatMap(new JoinPersonsWithAuctions()).name("Incremental join").setParallelism(params.getInt("p-join", 1));

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        joined.addSink(new DiscardingSink<>());

        // execute program
        env.execute("Nexmark Query3");
    }

    private static final class JoinPersonsWithAuctions extends RichCoFlatMapFunction<Auction, Person, Tuple4<String, String, String, Long>> {

        // person state: id, <name, city, state>
        private ValueState<Tuple3<String, String, String>> personMap;

        // auction state: seller, List<id>
        private ListState<Long> auctionMap;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple3<String, String, String>> personDescriptor =
                    new ValueStateDescriptor<Tuple3<String, String, String>>(
                            "person-state",
                            new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
                    );
            ListStateDescriptor<Long> auctionDescriptor =
                    new ListStateDescriptor<Long>(
                            "auction-state",
                            BasicTypeInfo.LONG_TYPE_INFO
                    );

            personMap = getRuntimeContext().getState(personDescriptor);
            auctionMap = getRuntimeContext().getListState(auctionDescriptor);
        }

        @Override
        public void flatMap1(Auction auction, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
            // check if auction has a match in the person state
            Tuple3<String, String, String> person = personMap.value();
            if (person != null) {
                // emit and don't store
                out.collect(new Tuple4<>(person.f0, person.f1, person.f2, auction.id));
            }
            else {
                // we need to store this auction for future matches
                auctionMap.add(auction.id);
            }
        }

        @Override
        public void flatMap2(Person person, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
            // store person in state
            if (personMap.value() == null) {
                personMap.update(new Tuple3<>(person.name, person.city, person.state));
            }
            Iterable<Long> auctionIds = auctionMap.get();
            for (Long auctionId: auctionIds) {
                out.collect(new Tuple4<>(person.name, person.city, person.state, auctionId));
            }
            auctionMap.clear();
        }
    }
}