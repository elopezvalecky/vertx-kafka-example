package nl.kii.eventstore.v2

import com.fasterxml.jackson.databind.SerializationFeature
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.json.Json
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import java.util.Properties
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier
import nl.kii.kafka.EmbeddedSingleNodeKafkaCluster
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
import io.vertx.core.AbstractVerticle
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.eclipse.xtend.lib.annotations.Accessors
import org.apache.kafka.streams.KafkaStreams

@RunWith(VertxUnitRunner)
class KafkaStreamsRunnerTest {
    
    @ClassRule
    val public static CLUSTER = new EmbeddedSingleNodeKafkaCluster

    @BeforeClass
    def static void setUpJVM() {
        System.setProperty('vertx.logger-delegate-factory-class-name','io.vertx.core.logging.SLF4JLogDelegateFactory')
        System.setProperty('hazelcast.logging.type', 'slf4j')
    }
    
    @BeforeClass
    def static void setUpObjectMapper() {
        Json.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        Json.prettyMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    }

    val protected Supplier<Vertx> supplier = [
        val latch = new CountDownLatch(1)
        val vertx = new AtomicReference<Vertx>
        Vertx.clusteredVertx(new VertxOptions().setClusterHost('127.0.0.1')) [
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

    @Rule public val rule = new RunTestOnContext(supplier)

    val config = new Properties => [
        put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
        put(ACKS_CONFIG, 'all')
        put(AUTO_OFFSET_RESET_CONFIG, 'earliest')
        put(APPLICATION_ID_CONFIG, 'test')
        put(BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers)
    ]

    @Test
    def void deploy(TestContext context) {
        val verticle = new KafkaStreamsRunner(config, #[])
        rule.vertx.deployVerticle(verticle, context.asyncAssertSuccess)        
    }
    
    @Test
    def void failedDeployWithExtraVerticles(TestContext context) {
        val verticle = new KafkaStreamsRunner(config, #[new TestVerticle])
        rule.vertx.deployVerticle(verticle, context.asyncAssertFailure)        
    }
    
    @Test
    def void deployWithExtraVerticles(TestContext context) {
        val verticle = new KafkaStreamsRunner(config, #[new StreamsTestVerticle])
        rule.vertx.deployVerticle(verticle, context.asyncAssertSuccess)        
    }

    static class TestVerticle extends AbstractVerticle {}

    @Accessors(PUBLIC_SETTER, PUBLIC_GETTER)
    static class StreamsTestVerticle extends AbstractVerticle {
        var UUID instanceId
        var KStreamBuilder builder
        var KafkaStreams streams
    }

}