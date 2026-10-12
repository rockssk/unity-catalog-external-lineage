package demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;

import java.time.Instant;
import java.util.*;

/**
 * Derives OpenLineage lineage from a Kafka Streams {@link Topology} and publishes it to the
 * openlineage.events topic. Inputs and outputs come from the topology itself (source/sink nodes),
 * so the lineage cannot drift from the code. Internal repartition/changelog topics are skipped.
 *
 * Column-level lineage comes from the field map the app uses for its own transformation:
 * outputTopic -> (outputField -> [inputFields]).
 */
public final class OpenLineageEmitter {
    private static final ObjectMapper M = new ObjectMapper();
    private static final String PRODUCER = "https://github.com/example/kafka-streams-openlineage";

    private final String bootstrap, namespace, lineageTopic;

    public OpenLineageEmitter(String bootstrap, String lineageTopic) {
        this.bootstrap = bootstrap;
        this.namespace = "kafka://" + bootstrap;
        this.lineageTopic = lineageTopic;
    }

    public String emit(String eventType, String jobName, Topology topology,
                       Map<String, List<String>> inputSchemas,
                       Map<String, Map<String, List<String>>> columnLineage) throws Exception {
        TopologyDescription desc = topology.describe();
        Set<String> inputs = new TreeSet<>(), outputs = new TreeSet<>();
        List<String> chains = new ArrayList<>();
        for (TopologyDescription.Subtopology st : desc.subtopologies()) {
            List<String> steps = new ArrayList<>();
            for (TopologyDescription.Node n : st.nodes()) {
                if (n instanceof TopologyDescription.Source s) {
                    s.topicSet().stream().filter(t -> !internal(t)).forEach(inputs::add);
                    steps.add("source(" + String.join(",", s.topicSet()) + ")");
                } else if (n instanceof TopologyDescription.Sink k) {
                    if (!internal(k.topic())) outputs.add(k.topic());
                    steps.add("sink(" + k.topic() + ")");
                } else {
                    steps.add(n.name());
                }
            }
            chains.add("[" + st.id() + "] " + String.join(" -> ", steps));
        }

        ObjectNode ev = M.createObjectNode();
        ev.put("eventType", eventType);
        ev.put("eventTime", Instant.now().toString());
        ev.put("producer", PRODUCER);
        ev.put("schemaURL", "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent");
        ev.putObject("run").put("runId", UUID.randomUUID().toString());
        ObjectNode job = ev.putObject("job");
        job.put("namespace", "kafka-streams://" + bootstrap);
        job.put("name", jobName);
        ObjectNode jf = job.putObject("facets");
        jf.putObject("jobType").put("processingType", "STREAMING").put("integration", "KAFKA_STREAMS").put("jobType", "TOPOLOGY");
        jf.putObject("documentation").put("description", "Kafka Streams topology: " + String.join(" | ", chains));

        ArrayNode in = ev.putArray("inputs");
        for (String t : inputs) {
            ObjectNode d = dataset(in, t);
            ArrayNode fields = ((ObjectNode) d.get("facets")).putObject("schema").putArray("fields");
            for (String f : inputSchemas.getOrDefault(t, List.of())) fields.addObject().put("name", f).put("type", "string");
        }
        ArrayNode out = ev.putArray("outputs");
        for (String t : outputs) {
            ObjectNode d = dataset(out, t);
            ObjectNode facets = ((ObjectNode) d.get("facets"));
            Map<String, List<String>> cl = columnLineage.getOrDefault(t, Map.of());
            ArrayNode fields = facets.putObject("schema").putArray("fields");
            ObjectNode clFields = facets.putObject("columnLineage").putObject("fields");
            for (var e : cl.entrySet()) {
                fields.addObject().put("name", e.getKey()).put("type", "string");
                ArrayNode inputFields = clFields.putObject(e.getKey()).putArray("inputFields");
                for (String src : e.getValue()) {
                    // input fields are "topic.field"
                    int dot = src.lastIndexOf('.');
                    inputFields.addObject().put("namespace", namespace)
                            .put("name", src.substring(0, dot)).put("field", src.substring(dot + 1));
                }
            }
        }

        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", StringSerializer.class.getName());
        String json = M.writeValueAsString(ev);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            producer.send(new ProducerRecord<>(lineageTopic, jobName, json)).get();
        }
        return json;
    }

    private ObjectNode dataset(ArrayNode arr, String topic) {
        ObjectNode d = arr.addObject();
        d.put("namespace", namespace);
        d.put("name", topic);
        d.putObject("facets").putObject("demo_entityType").put("type", "Topic");
        return d;
    }

    private static boolean internal(String topic) {
        return topic.endsWith("-repartition") || topic.endsWith("-changelog");
    }
}
