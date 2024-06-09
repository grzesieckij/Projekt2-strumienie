import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.KeyValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

public class CrimeStatsApp {

    private static Map<String, IUCRCode> iucrCodeMap = new HashMap<>();

    public static void main(String[] args) {
        if (args.length < 7) {
            System.err.println("Usage: java CrimeStatsApp <bootstrap-servers> <input-topic> <output-topic> <anomalies-topic> <iucr-codes-path> <D> <P>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String inputTopic = args[1];
        String outputTopic = args[2];
        String anomaliesTopic = args[3];
        String iucrCodesPath = args[4];
        int D = Integer.parseInt(args[5]); // Number of days for the window
        double P = Double.parseDouble(args[6]); // Percentage threshold

        loadIUCRCodes(iucrCodesPath);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "crime-stats-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Set a unique state directory for this instance
        String stateDir = "/tmp/kafka-streams/" + System.nanoTime();
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        // Custom Serde for CrimeRecord, CrimeStats, and AnomalyRecord
        Serde<CrimeRecord> crimeRecordSerde = Serdes.serdeFrom(new JsonPOJOSerializer<>(), new JsonPOJODeserializer<>(CrimeRecord.class));
        Serde<CrimeStats> crimeStatsSerde = Serdes.serdeFrom(new JsonPOJOSerializer<>(), new JsonPOJODeserializer<>(CrimeStats.class));
        Serde<AnomalyRecord> anomalyRecordSerde = Serdes.serdeFrom(new JsonPOJOSerializer<>(), new JsonPOJODeserializer<>(AnomalyRecord.class));

        KStream<String, CrimeRecord> crimes = lines
                .mapValues(line -> {
                    CrimeRecord record = parseCrimeRecord(line);
                    if (record != null) {
                        enrichCrimeRecord(record);
                        System.out.println("Parsed Crime Record: " + record);
                    }
                    return record;
                })
                .filter((key, value) -> value != null)
                .transformValues(new CrimeRecordTimestampExtractor());

        crimes.foreach((key, value) -> System.out.println("Processed Crime Record: " + value));

        KGroupedStream<String, CrimeRecord> groupedByDistrict = crimes.groupBy(
                (key, value) -> value.District,
                Grouped.with(Serdes.String(), crimeRecordSerde)
        );

        // Tumbling window
        KTable<Windowed<String>, CrimeStats> windowedCrimeStats = groupedByDistrict
                .windowedBy(TimeWindows.of(Duration.ofDays(D)))
                .aggregate(
                        CrimeStats::new,
                        (key, value, aggregate) -> {
                            aggregate.District = key;
                            aggregate.TotalCrimes++;
                            if (value.Arrest) aggregate.Arrests++;
                            if (value.Domestic) aggregate.DomesticCrimes++;
                            if (isFBIReportable(value.IUCR)) aggregate.FBICrimes++;
                            return aggregate;
                        },
                        Materialized.<String, CrimeStats, WindowStore<Bytes, byte[]>>as("windowed-crime-stats")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(crimeStatsSerde)
                );

        windowedCrimeStats.toStream().foreach((key, value) -> System.out.println("Aggregated Crime Stats: " + key + " -> " + value));

        KStream<String, AnomalyRecord> anomalies = windowedCrimeStats
                .toStream()
                .filter((key, value) -> {
                    double percentage = ((double) value.FBICrimes / value.TotalCrimes) * 100;
                    return value.TotalCrimes > 0 && percentage > P; // Ensure meaningful percentage calculation
                })
                .map((key, value) -> {
                    AnomalyRecord anomaly = new AnomalyRecord();
                    anomaly.start = key.window().startTime().toString();
                    anomaly.end = key.window().endTime().toString();
                    anomaly.district = key.key(); // Correctly setting the district from the key
                    anomaly.fbiCrimes = value.FBICrimes;
                    anomaly.totalCrimes = value.TotalCrimes;
                    anomaly.percentage = ((double) value.FBICrimes / value.TotalCrimes) * 100;
                    return KeyValue.pair(anomaly.district, anomaly);
                });

        anomalies.foreach((k, v) -> System.out.printf("%s; %s\n", k, v));
        anomalies.to(anomaliesTopic, Produced.with(Serdes.String(), anomalyRecordSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler(new StreamsUncaughtExceptionHandler() {
            @Override
            public StreamThreadExceptionResponse handle(Throwable throwable) {
                throwable.printStackTrace();
                System.out.println("Exception occurred in StreamThread: " + throwable.getMessage());
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            }
        });

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                Files.deleteIfExists(Paths.get(stateDir));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.exit(0);
    }

    private static CrimeRecord parseCrimeRecord(String line) {
        String[] fields = line.split(",");
        if (fields.length < 6) {
            System.err.println("Invalid record: " + line);
            return null;
        }
        CrimeRecord record = new CrimeRecord();
        record.ID = fields[0];
        record.Date = fields[1];
        record.IUCR = fields[2];
        record.Arrest = Boolean.parseBoolean(fields[3]);
        record.Domestic = Boolean.parseBoolean(fields[4]);
        record.District = fields[5];
        return record;
    }

    private static void enrichCrimeRecord(CrimeRecord record) {
        IUCRCode code = iucrCodeMap.get(record.IUCR);
        if (code != null) {
            record.PrimaryDescription = code.PrimaryDescription;
        } else {
            record.PrimaryDescription = "UNKNOWN";
        }
        DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        try {
            if (record.Date != null && !record.Date.isEmpty()) {
                ZonedDateTime date = ZonedDateTime.parse(record.Date, formatter);
                record.Month = date.getYear() + "-" + String.format("%02d", date.getMonthValue());
            } else {
                record.Month = "UNKNOWN";
            }
        } catch (DateTimeParseException e) {
            System.err.println("Error parsing date: " + record.Date + " - " + e.getMessage());
            record.Month = "UNKNOWN";
        }
    }

    private static boolean isFBIReportable(String iucr) {
        IUCRCode code = iucrCodeMap.get(iucr);
        return code != null && "I".equals(code.IndexCode);
    }

    private static void loadIUCRCodes(String iucrCodesPath) {
        try (Stream<String> lines = Files.lines(Paths.get(iucrCodesPath))) {
            lines
                    .skip(1) // Skip header
                    .map(line -> line.split(","))
                    .forEach(columns -> {
                        if (columns.length == 4) {
                            IUCRCode code = new IUCRCode();
                            code.IUCR = columns[0].length() < 4 ? "0" + columns[0] : columns[0];
                            code.PrimaryDescription = columns[1];
                            code.SecondaryDescription = columns[2];
                            code.IndexCode = columns[3];
                            iucrCodeMap.put(code.IUCR, code);
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
