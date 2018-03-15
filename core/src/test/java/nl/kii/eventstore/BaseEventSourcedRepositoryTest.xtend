package nl.kii.eventstore

import com.fasterxml.jackson.databind.SerializationFeature
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.json.Json
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier
import nl.kii.kafka.EmbeddedSingleNodeKafkaCluster
import org.junit.BeforeClass
import org.junit.ClassRule

abstract class BaseEventSourcedRepositoryTest {

    val protected static TOPIC = 'eventLog'
    val protected static STORE = 'aggregateStore'

    @ClassRule
    val public static CLUSTER = new EmbeddedSingleNodeKafkaCluster
    
    val protected static MAX_WAIT_MS = 10000

    @BeforeClass
    def static void setUpTopics() {
        CLUSTER.createTopic(TOPIC)
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

}