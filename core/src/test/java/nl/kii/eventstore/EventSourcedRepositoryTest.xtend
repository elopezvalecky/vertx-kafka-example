package nl.kii.eventstore

import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus
import io.vertx.core.json.JsonObject
import io.vertx.core.net.impl.ServerID
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import java.util.UUID
import org.apache.kafka.test.TestUtils
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

import static org.apache.kafka.clients.consumer.ConsumerConfig.*
import static org.apache.kafka.clients.producer.ProducerConfig.*
import static org.apache.kafka.streams.StreamsConfig.*
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
import static org.apache.kafka.streams.StreamsConfig.CLIENT_ID_CONFIG

@RunWith(VertxUnitRunner)
class EventSourcedRepositoryTest extends BaseEventSourcedRepositoryTest {

    @BeforeClass
    def static void setUpTopics() {
        CLUSTER.createTopic(TOPIC)
    }

    @Rule public val rule = new RunTestOnContext(supplier)

    var EventSourcedRepository repo

    @Before
    def void setUpStreamsAndProducer(TestContext context) {
        val node = rule.vertx.getServerID
        val props = new JsonObject => [
            put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
            put(ACKS_CONFIG, 'all')
            put(AUTO_OFFSET_RESET_CONFIG, 'earliest')
            put(APPLICATION_ID_CONFIG, 'test')
            put(APPLICATION_SERVER_CONFIG, '''«node.host»:«node.port»'''.toString)
            put(BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers)
        ]
        
        repo = new EventSourcedRepository(rule.vertx, TOPIC, STORE, props.map)
    }
    
    @After
    def void tearDownStreamsAndProducer() {
        this.repo.close
    }

    @Test
    def void saveAndLoad(TestContext context) {
        val async = context.async
        
        // Wait until the StreamsMetadata is available as this indicates that KafkaStreams initialization has occurred
        TestUtils.waitForCondition([repo.isStoreReady], MAX_WAIT_MS, 'StreamsMetadata should be available')        

        val demo = new Demo(UUID.randomUUID, 'Demo')
        
        Future.succeededFuture
            .compose [repo.save(demo, null)]
            .compose [repo.save(demo.increase, null)]
            .compose [repo.save(demo.toggle, null)]
            .compose [repo.save(demo.increase, null)]
            .compose [repo.save(demo.increase, null)]
            .compose [repo.save(demo.decrease, null)]
            .compose [repo.save(demo.text('Test'), null)]
            .compose [repo.save(demo.increase, null)]
            .compose [repo.save(demo.toggle, null)]
            .compose [
                Thread.sleep(1000)
                repo.load(demo.id, Demo)
            ]
            .setHandler [
                if (failed) {
                    context.fail(cause)
                    return
                }
                context.assertEquals(3, result.number)
                context.assertTrue(result.flag)
                context.assertEquals('Test', result.text)
                async.complete
            ]
    }

    private def getServerID(Vertx vertx) {
        val serverIDField  = ClusteredEventBus.getDeclaredField('serverID') => [
            accessible = true
        ]
        serverIDField.get(vertx.eventBus) as ServerID
    }
    
}
