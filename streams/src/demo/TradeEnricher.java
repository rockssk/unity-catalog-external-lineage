package demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.*;

/**
 * trade-enricher: OMS raw orders -> normalized trade executions + per-fund net flows.
 *
 *   oms.orders.raw ──filter FILLED──normalize──▶ trades.executions
 *                  └──signed notional──groupBy fund──sum──▶ trades.fund_flows
 *
 * On start it publishes its own OpenLineage event (derived from the topology) to openlineage.events.
 */
public final class TradeEnricher {
    static final String RAW = "oms.orders.raw", TRADES = "trades.executions", FLOWS = "trades.fund_flows";
    static final ObjectMapper M = new ObjectMapper();

    /** output field -> raw field. Single source of truth for both the transform and the column lineage. */
    static final LinkedHashMap<String, String> TRADE_FIELDS = new LinkedHashMap<>();
    static {
        TRADE_FIELDS.put("trade_id", "ord_id");
        TRADE_FIELDS.put("account_id", "acct");
        TRADE_FIELDS.put("fund_id", "ticker");
        TRADE_FIELDS.put("side", "side");
        TRADE_FIELDS.put("quantity", "qty");
        TRADE_FIELDS.put("price", "px");
        TRADE_FIELDS.put("trade_ts", "ts");
    }

    static Topology build() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, JsonNode> filled = b.stream(RAW, Consumed.with(Serdes.String(), Serdes.String()).withName("read-oms-orders"))
                .mapValues(TradeEnricher::parse, Named.as("parse-json"))
                .filter((k, v) -> v != null && "FILLED".equals(v.path("status").asText()), Named.as("filter-status-FILLED"));

        filled.mapValues(TradeEnricher::normalize, Named.as("normalize-trade-fields"))
              .to(TRADES, Produced.with(Serdes.String(), Serdes.String()).withName("write-trade-executions"));

        filled.selectKey((k, v) -> v.path("ticker").asText(), Named.as("key-by-fund"))
              .mapValues(v -> ("SELL".equalsIgnoreCase(v.path("side").asText()) ? -1 : 1) * v.path("qty").asDouble() * v.path("px").asDouble(),
                         Named.as("signed-notional"))
              .groupByKey(Grouped.with("by-fund", Serdes.String(), Serdes.Double()))
              .reduce(Double::sum, Named.as("sum-net-notional"), Materialized.as("fund-flows-store"))
              .toStream(Named.as("flows-changelog-to-stream"))
              .mapValues((fund, net) -> "{\"fund_id\":\"" + fund + "\",\"net_notional\":" + Math.round(net * 100) / 100.0 + "}",
                         Named.as("to-flow-json"))
              .to(FLOWS, Produced.with(Serdes.String(), Serdes.String()).withName("write-fund-flows"));
        return b.build();
    }

    static JsonNode parse(String s) {
        try { return M.readTree(s); } catch (Exception e) { return null; }
    }

    static String normalize(JsonNode v) {
        ObjectNode o = M.createObjectNode();
        TRADE_FIELDS.forEach((out, in) -> o.set(out, v.get(in)));
        o.put("side", v.path("side").asText().toUpperCase());
        // pass through unmapped upstream fields, so new OMS attributes flow downstream (and schema changes cascade)
        v.fields().forEachRemaining(e -> {
            if (!TRADE_FIELDS.containsValue(e.getKey()) && !"status".equals(e.getKey())) o.set(e.getKey(), e.getValue());
        });
        return o.toString();
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP", "localhost:9092");
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "trade-enricher");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(StreamsConfig.STATE_DIR_CONFIG, System.getenv().getOrDefault("STATE_DIR", System.getProperty("user.dir") + "/.kafka/streams-state"));
        p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        p.put("auto.offset.reset", "earliest");

        Topology topo = build();
        System.out.println(topo.describe());

        Map<String, Map<String, List<String>>> colLineage = new HashMap<>();
        Map<String, List<String>> tradeCols = new LinkedHashMap<>();
        TRADE_FIELDS.forEach((out, in) -> tradeCols.put(out, List.of(RAW + "." + in)));
        colLineage.put(TRADES, tradeCols);
        colLineage.put(FLOWS, new LinkedHashMap<>(Map.of(
                "fund_id", List.of(RAW + ".ticker"),
                "net_notional", List.of(RAW + ".qty", RAW + ".px", RAW + ".side"))));
        Map<String, List<String>> inputSchemas = Map.of(RAW, List.of("ord_id", "acct", "ticker", "side", "qty", "px", "ts", "status"));

        KafkaStreams streams = new KafkaStreams(topo, p);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close(java.time.Duration.ofSeconds(10))));
        streams.start();
        new OpenLineageEmitter(bootstrap, "openlineage.events").emit("START", "trade-enricher", topo, inputSchemas, colLineage);
        System.out.println("trade-enricher running; OpenLineage START event published to openlineage.events");
        Thread.currentThread().join();
    }
}
