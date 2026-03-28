package oms.bus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import oms.audit.GatewayAuditEvent;
import oms.audit.GatewayAuditIntakeService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class BusEventIntakeService {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final GatewayAuditIntakeService auditIntakeService;
    private final boolean enabled;
    private final boolean kafkaEnabled;
    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final long pollMs;
    private final Instant startedAt = Instant.now();
    private final AtomicReference<String> state = new AtomicReference<>("IDLE");
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong deadLetters = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();
    private final AtomicLong lastEventAt = new AtomicLong();

    public BusEventIntakeService(GatewayAuditIntakeService auditIntakeService) {
        this.auditIntakeService = auditIntakeService;
        this.enabled = parseBoolean(System.getenv().getOrDefault("OMS_BUS_ENABLE", "true"));
        this.kafkaEnabled = parseBoolean(System.getenv().getOrDefault("OMS_BUS_KAFKA_ENABLE", "false"));
        this.bootstrapServers = System.getenv().getOrDefault(
            "OMS_BUS_KAFKA_BOOTSTRAP_SERVERS",
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        );
        this.topic = System.getenv().getOrDefault(
            "OMS_BUS_KAFKA_TOPIC",
            System.getenv().getOrDefault("KAFKA_TOPIC", "events")
        );
        this.groupId = System.getenv().getOrDefault("OMS_BUS_KAFKA_GROUP_ID", "oms-java");
        this.pollMs = Long.parseLong(System.getenv().getOrDefault("OMS_BUS_KAFKA_POLL_MS", "500"));
    }

    public void start() {
        if (!enabled) {
            state.set("DISABLED");
            return;
        }
        if (!kafkaEnabled) {
            state.set("HTTP_ONLY");
            return;
        }
        Thread worker = new Thread(this::runKafkaLoop, "oms-java-bus-kafka");
        worker.setDaemon(true);
        worker.start();
    }

    public IntakeStatus snapshot() {
        return new IntakeStatus(
            enabled,
            kafkaEnabled,
            state.get(),
            topic,
            groupId,
            startedAt,
            received.get(),
            applied.get(),
            duplicates.get(),
            pending.get(),
            deadLetters.get(),
            errors.get(),
            lastEventAt.get() == 0 ? null : lastEventAt.get()
        );
    }

    public IngestResult ingest(BusEventV2 event) {
        if (!enabled) {
            return new IngestResult("DISABLED", event == null ? null : event.eventId(), false, "bus_disabled");
        }
        if (event == null) {
            errors.incrementAndGet();
            return new IngestResult("INVALID", null, false, "event_missing");
        }
        try {
            if (event.schemaVersion() < 2) {
                errors.incrementAndGet();
                return new IngestResult("INVALID", event.eventId(), false, "unsupported_schema_version");
            }
            String eventRef = buildEventRef(event);
            GatewayAuditIntakeService.IngestResult result = auditIntakeService.ingestSequencedEvent(
                toGatewayAuditEvent(event),
                eventRef,
                OBJECT_MAPPER.writeValueAsString(event),
                firstNonBlank(event.aggregateId(), event.orderId()),
                event.aggregateSeq()
            );
            received.incrementAndGet();
            lastEventAt.accumulateAndGet(parseEpochMillis(event.occurredAt()), Math::max);
            recordStatus(result.status());
            return new IngestResult(result.status(), eventRef, result.applied(), null);
        } catch (Exception exception) {
            errors.incrementAndGet();
            return new IngestResult("ERROR", event.eventId(), false, exception.getMessage());
        }
    }

    private void runKafkaLoop() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            state.set("RUNNING");
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Math.max(100L, pollMs)));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        BusEventV2 event = OBJECT_MAPPER.readValue(record.value(), BusEventV2.class);
                        ingest(event);
                    } catch (Exception exception) {
                        errors.incrementAndGet();
                    }
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (Exception exception) {
            state.set("ERROR");
            errors.incrementAndGet();
            exception.printStackTrace(System.err);
            return;
        }
        state.set("STOPPED");
    }

    private void recordStatus(String status) {
        switch (status) {
            case "APPLIED" -> applied.incrementAndGet();
            case "DUPLICATE" -> duplicates.incrementAndGet();
            case "PENDING" -> pending.incrementAndGet();
            case "DLQ" -> deadLetters.incrementAndGet();
            default -> {
            }
        }
    }

    private static GatewayAuditEvent toGatewayAuditEvent(BusEventV2 event) {
        return new GatewayAuditEvent(
            event.eventType(),
            parseEpochMillis(event.occurredAt()),
            event.accountId(),
            firstNonBlank(event.orderId(), event.aggregateId()),
            event.data()
        );
    }

    private static long parseEpochMillis(String value) {
        return Instant.parse(value).toEpochMilli();
    }

    private static String buildEventRef(BusEventV2 event) {
        if (event.eventId() != null && !event.eventId().isBlank()) {
            return event.eventId();
        }
        return "bus:" + firstNonBlank(event.aggregateId(), "unknown") + ":" + event.aggregateSeq();
    }

    private static String firstNonBlank(String primary, String fallback) {
        return primary != null && !primary.isBlank() ? primary : fallback;
    }

    private static boolean parseBoolean(String value) {
        return value != null && ("1".equals(value) || "true".equalsIgnoreCase(value));
    }

    public record IntakeStatus(
        boolean enabled,
        boolean kafkaEnabled,
        String state,
        String topic,
        String groupId,
        Instant startedAt,
        long received,
        long applied,
        long duplicates,
        long pending,
        long deadLetters,
        long errors,
        Long lastEventAt
    ) {
    }

    public record IngestResult(
        String status,
        String eventRef,
        boolean applied,
        String error
    ) {
    }
}
