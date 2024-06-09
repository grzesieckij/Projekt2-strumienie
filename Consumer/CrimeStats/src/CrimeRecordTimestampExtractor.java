import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class CrimeRecordTimestampExtractor implements ValueTransformerWithKeySupplier<String, CrimeRecord, CrimeRecord> {
    @Override
    public ValueTransformerWithKey<String, CrimeRecord, CrimeRecord> get() {
        return new ValueTransformerWithKey<String, CrimeRecord, CrimeRecord>() {
            private ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
            }

            @Override
            public CrimeRecord transform(String key, CrimeRecord value) {
                DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
                try {
                    ZonedDateTime date = ZonedDateTime.parse(value.Date, formatter);
                    context.headers().add("record-timestamp", String.valueOf(date.toInstant().toEpochMilli()).getBytes());
                } catch (DateTimeParseException e) {
                    System.err.println("Error parsing date for record: " + value.Date + " - " + e.getMessage());
                }
                return value;
            }

            @Override
            public void close() {
            }
        };
    }
}
