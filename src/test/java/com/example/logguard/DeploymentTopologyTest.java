package com.example.logguard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class DeploymentTopologyTest {

    private static final Set<String> SHARD_QUEUES = Set.of(
        "guard.logs.queue.shard-0",
        "guard.logs.queue.shard-1",
        "guard.logs.queue.shard-2",
        "guard.logs.queue.shard-3"
    );

    @Test
    void composeStartsOneConsumerForEveryDeclaredShard() throws Exception {
        String compose = Files.readString(Path.of("docker-compose.yml"));

        for (int shard = 0; shard < 4; shard++) {
            assertThat(compose)
                .contains("log-guard-shard-" + shard + ":")
                .contains("SHARD_ID: " + shard);
        }
    }

    @Test
    void rabbitDefinitionsDeclareEveryShardQueueAndBinding() throws Exception {
        JsonNode definitions = new ObjectMapper().readTree(
            Path.of("src/main/resources/rabbitmq-definitions.json").toFile());

        Set<String> queues = new HashSet<>();
        definitions.path("queues").forEach(queue -> queues.add(queue.path("name").asText()));
        assertThat(queues).containsAll(SHARD_QUEUES);

        Set<String> shardBindings = new HashSet<>();
        definitions.path("bindings").forEach(binding -> {
            if ("guard.exchange".equals(binding.path("source").asText())) {
                shardBindings.add(binding.path("routing_key").asText()
                    + "->" + binding.path("destination").asText());
            }
        });

        for (int shard = 0; shard < 4; shard++) {
            assertThat(shardBindings).contains(
                "log.ingest.shard-" + shard + "->guard.logs.queue.shard-" + shard);
        }
    }
}
