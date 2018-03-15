package nl.kii.service.content

import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier
import nl.kii.kafka.EmbeddedSingleNodeKafkaCluster
import org.junit.Before
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.runner.RunWith

import static org.apache.kafka.clients.consumer.ConsumerConfig.*
import static org.apache.kafka.clients.producer.ProducerConfig.*
import static org.apache.kafka.streams.StreamsConfig.*
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG

@RunWith(VertxUnitRunner)
class ContentServiceTest {

    @ClassRule
    val public static CLUSTER = new EmbeddedSingleNodeKafkaCluster

    val protected static TOPIC = 'eventLog'
    val protected static STORE = 'aggregateStore'

    @BeforeClass
    def static void setUpTopics() {
        CLUSTER.createTopic(TOPIC)
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

    @Before
    def void setUpVerticle(TestContext context) {
        val options = new DeploymentOptions => [
            config = new JsonObject(#{
                'service.topic' -> TOPIC,
                'service.store' -> STORE,
                'kafka' -> #{
                    ACKS_CONFIG -> 'all',
                    AUTO_OFFSET_RESET_CONFIG -> 'earliest',
                    APPLICATION_ID_CONFIG -> 'test',
                    BOOTSTRAP_SERVERS_CONFIG -> CLUSTER.bootstrapServers
                }
            })
        ]
        rule.vertx.deployVerticle(ContentVerticle.name, options, context.asyncAssertSuccess)
    }

//    @Test
//    def void main(TestContext context) {
//        val proxy = ProxyHelper.createProxy(FeedCommandHandler, rule.vertx, FeedCommandHandler.ADDRESS)
//        
//        val id = UUID.randomUUID
//        val title = 'Test'
//        val url = new URL('http://example.com')
//        
//        Future.succeededFuture
//            .compose[
//                val future = Future.future
//                proxy.addRssFeed(new AddRssCommand(id, title, url), future.completer)
//                future
//            ]
//            .compose[
//                Thread.sleep(1900)
//                val future = Future.future
//                proxy.removeRssFeed(new RemoveRssCommand(id), future.completer)
//                future
//            ]
//            .setHandler(context.asyncAssertSuccess)
//    }

}
