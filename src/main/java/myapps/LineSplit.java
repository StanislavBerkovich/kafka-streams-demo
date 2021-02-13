package myapps;

import com.sun.tools.javac.util.Pair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LineSplit {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        final String first_table_name = "counts-store";
        final String second_table_name = "counts-store2";
        final String join_table_name = "join-table";

        KStream<String, String> first_source = builder.stream("input");

        KTable<String, Long> first_table = buildWordCountTable(builder, first_source, first_table_name);
        KStream<String, String> second_source = builder.stream("input2");
        KTable<String, Long> second_table = buildWordCountTable(builder, second_source, second_table_name);

        KTable<String, String> join_table = first_table
                .join(second_table, (value1, value2) -> value1 + " | " + value2,
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(join_table_name));
        join_table.toStream().to(join_table_name + "-table-stream", Produced.with(Serdes.String(), Serdes.String()));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(2);

        Runtime.getRuntime().addShutdownHook(shutdownHook(streams, latch));

        runStreamsThread(streams, latch).start();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        readTable(streams, first_table_name).start();
        readTable(streams, second_table_name).start();
        readTable(streams, join_table.queryableStoreName()).start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.exit(0);
    }

    private static KTable<String, Long> buildWordCountTable (StreamsBuilder builder, KStream<String, String> source, String tableName) {
        KTable<String, Long> table = source
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> {
                System.out.println("group" + " " + key + " " + value);
                return value;
            })
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(tableName));

        table.toStream().to(tableName + "-table-stream", Produced.with(Serdes.String(), Serdes.Long()));
        return table;
    }

    private static Thread shutdownHook (KafkaStreams streams, CountDownLatch latch) {
        return new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        };
    }

    private static Thread runStreamsThread (KafkaStreams streams, CountDownLatch latch) {
        return new Thread("runStreams") {
            @Override
            public void run() {
                try {
                    System.out.println("starthook");
                    streams.start();
                    latch.await();
                } catch (Throwable e) {
                    System.exit(1);
                }
            }
        };
    }

    private static Thread readTable (KafkaStreams streams, String name) throws InterruptedException {
        return new Thread("tableReading " + name) {
            @Override
            public void run() {
                System.out.println(name);

                while (true) {
                    ReadOnlyKeyValueStore<String, Long> keyValueStore =
                            streams.store(StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.keyValueStore()));
                    System.out.println(name +  " | count for stas:" + keyValueStore.get("stas"));
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
    }
}
