package nl.kii

import com.fasterxml.jackson.databind.SerializationFeature
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier
import nl.kii.eventsourcing.Message
import nl.kii.eventstore.AggregateRootDeserializer
import nl.kii.eventstore.AggregateRootSerializer
import nl.kii.eventstore.DomainEventMessageDeserializer
import nl.kii.eventstore.DomainEventMessageSerializer
import nl.kii.eventstore.JavaDeserializer
import nl.kii.eventstore.JavaSerializer
import nl.kii.kafka.EmbeddedSingleNodeKafkaCluster
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.state.Stores
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

import static org.apache.kafka.clients.consumer.ConsumerConfig.*
import static org.apache.kafka.clients.producer.ProducerConfig.*
import static org.apache.kafka.streams.StreamsConfig.*
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
import static org.apache.kafka.streams.StreamsConfig.CLIENT_ID_CONFIG

@RunWith(VertxUnitRunner)
class AggregationTest {
    
    val static COMMAND_TOPIC = 'commands'
    val static EVENT_TOPIC = 'events'

    @ClassRule
    val public static CLUSTER = new EmbeddedSingleNodeKafkaCluster

    @BeforeClass
    def static void setUpJVM() {
        System.setProperty('vertx.logger-delegate-factory-class-name','io.vertx.core.logging.SLF4JLogDelegateFactory')
    }

    @BeforeClass
    def static void setUpTopics() {
        CLUSTER.createTopic(COMMAND_TOPIC, 2, 1)
        CLUSTER.createTopic(EVENT_TOPIC, 2, 1)
    }

    @BeforeClass
    def static void setUpObjectMapper() {
        Json.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        Json.prettyMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    }
    
    val Supplier<Vertx> supplier = [
        val latch = new CountDownLatch(1)
        val vertx = new AtomicReference<Vertx>
        Vertx.clusteredVertx(new VertxOptions()) [
            if(failed) throw new RuntimeException('Unable to create clustered Vertx')
            vertx.set(result)
            latch.countDown
        ]
        try {
            latch.await
        } catch (InterruptedException e) {
            throw new RuntimeException(e)
        }
        vertx.get
    ]

    @Rule public val vertx = new RunTestOnContext(supplier)

    @Test
    def void aggregation(TestContext context) {
        val props = new JsonObject => [
            put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
            put(ACKS_CONFIG, 'all')
            put(AUTO_OFFSET_RESET_CONFIG, 'earliest')
            put(APPLICATION_ID_CONFIG, 'test')
            put(BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers)
        ]
        
        val aggregateSerde = Serdes.serdeFrom(new AggregateRootSerializer, new AggregateRootDeserializer)
        val commandSerde = Serdes.serdeFrom(new JavaSerializer<Message<?>>, new JavaDeserializer<Message<?>>)
        val eventSerde = Serdes.serdeFrom(new DomainEventMessageSerializer, new DomainEventMessageDeserializer)
        
        val builder = new KStreamBuilder
        
        val aggregateStore = Stores.create('aggregate-state')
                                  .withKeys(Serdes.String)
                                  .withValues(aggregateSerde)
                                  .persistent
                                  .build
        builder.addStateStore(aggregateStore)        
        
        val stream = builder.stream(Serdes.String, commandSerde, COMMAND_TOPIC)
        
        // Add Rss
//        stream
//            .filter[key,value|value.payloadType == AddRssCommand]
//            .mapValues[value|
//                val extension payload = value.payload as AddRssCommand
//                new Rss(id, title, url)
//            ]
//            .flatMapValues[aggregate|
//                aggregate
//                    .unsaved
//                    .map[new GenericDomainEventMessage(aggregate.id, aggregate.class.name, it, emptyMap) as DomainEventMessage<? extends Event>]
//            ]
//            .to(Serdes.String, eventSerde, EVENT_TOPIC)
            
        
        val streams = new KafkaStreams(builder, new StreamsConfig(props.map))
        streams.cleanUp
        streams.start
    }
    
    @FinalFieldsConstructor
    static class AddRssCommandHandler {
        val KStream<String, Message<?>> stream
        
        
    }

}