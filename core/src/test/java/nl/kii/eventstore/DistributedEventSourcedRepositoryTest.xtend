package nl.kii.eventstore

import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus
import io.vertx.core.impl.CompositeFutureImpl
import io.vertx.core.json.JsonObject
import io.vertx.core.net.impl.ServerID
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.Repeat
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import java.util.UUID
import org.apache.kafka.test.TestUtils
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
class DistributedEventSourcedRepositoryTest extends BaseEventSourcedRepositoryTest {

    @BeforeClass
    def static void setUpTopics() {
        CLUSTER.createTopic(TOPIC,2,1)
    }

    @Rule public val rule0 = new RunTestOnContext(supplier)
    @Rule public val rule1 = new RunTestOnContext(supplier)
    @Rule public val rule2 = new RunTestOnContext(supplier)

    @Test
    @Repeat(3)
    def void distribution(TestContext context) {
        val async = context.async

        val kConf = new JsonObject => [
            put(ACKS_CONFIG, 'all')
            put(AUTO_OFFSET_RESET_CONFIG, 'earliest')
            put(APPLICATION_ID_CONFIG, 'repository-test')
            put(BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers)
        ]
        
        val vertx1 = rule1.vertx
        val node1 = vertx1.getServerID
        val kConf1 = kConf.copy => [
            put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
            put(APPLICATION_SERVER_CONFIG, node1.toString)
        ]
        val repo1 = new EventSourcedRepository(rule1.vertx, TOPIC, STORE, kConf1.map)

        val vertx2 = rule2.vertx
        val node2 = vertx2.getServerID
        val kConf2 = kConf.copy => [
            put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
            put(APPLICATION_SERVER_CONFIG, node2.toString)
        ]

        val repo2 = new EventSourcedRepository(rule1.vertx, TOPIC, STORE, kConf2.map)
        
        // Wait until the StreamsMetadata is available as this indicates that KafkaStreams initialization has occurred
        TestUtils.waitForCondition([repo1.isStoreReady || repo2.isStoreReady], MAX_WAIT_MS, 'StreamsMetadata should be available')

        val data = (1..20).map[new Demo(UUID.randomUUID, 'Test#'+it)].toList
        
        data.subList(0,8).forEach[it.increase.toggle.increase.text(it.text+'$Modified').increase]
        data.subList(9,12).forEach[it.toggle.increase.decrease]
        data.subList(13,19).forEach[it.increase.toggle.decrease.toggle.increase.toggle]

        Future.succeededFuture
            .compose [
                val future = Future.<Void>future
                rule0.vertx.executeBlocking([
                    val saved = data.map[repo1.save(it, null)]
                    Thread.sleep(1000)
                    CompositeFutureImpl.all(saved).map[null].setHandler(completer)                    
                ], future.completer)
                future
            ]
            .compose [
                val future = Future.future
                rule0.vertx.executeBlocking([
                    Thread.sleep(1000)
                    repo2.load(data.get(5).id, Demo).setHandler(completer)
                ],future.completer)
                future
            ]
            .map [
                context.assertEquals(data.get(5).id, id)
                context.assertEquals(data.get(5).text, text)
                context.assertEquals(data.get(5).number, number)
                context.assertEquals(data.get(5).flag, flag)
                null
            ]
            .compose [
                val future = Future.future
                rule0.vertx.executeBlocking([
                    repo2.load(data.get(17).id, Demo).setHandler(completer)
                ],future.completer)
                future
            ]
            .map [
                context.assertEquals(data.get(17).id, id)
                context.assertEquals(data.get(17).text, text)
                context.assertEquals(data.get(17).number, number)
                context.assertEquals(data.get(17).flag, flag)
                null
            ]
            .setHandler [
                if (failed) context.fail(cause)
                else {
                    repo1.close
                    repo2.close
                    async.complete
                }
            ]
    }

    private def getServerID(Vertx vertx) {
        val serverIDField  = ClusteredEventBus.getDeclaredField('serverID') => [
            accessible = true
        ]
        serverIDField.get(vertx.eventBus) as ServerID
    }

}