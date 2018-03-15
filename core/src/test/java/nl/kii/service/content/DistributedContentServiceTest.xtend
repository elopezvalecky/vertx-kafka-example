package nl.kii.service.content

import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import io.vertx.serviceproxy.ServiceProxyBuilder
import java.net.URL
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier
import nl.kii.kafka.EmbeddedSingleNodeKafkaCluster
import nl.kii.service.content.command.AddRssCommand
import nl.kii.service.content.command.FeedCommandHandler
import nl.kii.service.content.command.RemoveRssCommand
import org.junit.Before
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

import static org.apache.kafka.clients.consumer.ConsumerConfig.*
import static org.apache.kafka.clients.producer.ProducerConfig.*
import static org.apache.kafka.streams.StreamsConfig.*
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG

@RunWith(VertxUnitRunner)
class DistributedContentServiceTest {

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

    @Rule public val rule1 = new RunTestOnContext(supplier)
    @Rule public val rule2 = new RunTestOnContext(supplier)

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
        rule1.vertx.deployVerticle(ContentVerticle.name, options, context.asyncAssertSuccess)
        rule2.vertx.deployVerticle(ContentVerticle.name, options, context.asyncAssertSuccess)
    }

    @Test
    def void main(TestContext context) {
        val async = context.async
        val proxy1 = new ServiceProxyBuilder(rule1.vertx).setAddress(FeedCommandHandler.ADDRESS).build(FeedCommandHandler)
        val proxy2 = new ServiceProxyBuilder(rule2.vertx).setAddress(FeedCommandHandler.ADDRESS).build(FeedCommandHandler)
        
        val id = UUID.randomUUID
        
        Future.succeededFuture
            .compose[
                val future = Future.future
                proxy1.addRssFeed(new AddRssCommand(UUID.randomUUID, 'Test#1', new URL('http://example.com/1')), future.completer)
                future
            ]
            .compose[
                val future = Future.future
                proxy1.addRssFeed(new AddRssCommand(UUID.randomUUID, 'Test#2', new URL('http://example.com/2')), future.completer)
                future
            ]
            .compose[
                val future = Future.future
                proxy1.addRssFeed(new AddRssCommand(UUID.randomUUID, 'Test#3', new URL('http://example.com/3')), future.completer)
                future
            ]
            .compose[
                val future = Future.future
                proxy1.addRssFeed(new AddRssCommand(id, 'Test#0', new URL('http://example.com/0')), future.completer)
                future
            ]
            .compose[
                val future = Future.future
                proxy1.addRssFeed(new AddRssCommand(UUID.randomUUID, 'Test#4', new URL('http://example.com/4')), future.completer)
                future
            ]
            .compose[
                val future = Future.future
                proxy1.addRssFeed(new AddRssCommand(UUID.randomUUID, 'Test#5', new URL('http://example.com/5')), future.completer)
                future
            ]
            .compose[
                val future = Future.future
                proxy1.addRssFeed(new AddRssCommand(UUID.randomUUID, 'Test#6', new URL('http://example.com/6')), future.completer)
                future
            ]
            .compose[
                Thread.sleep(1900)
                val future = Future.future
                proxy2.removeRssFeed(new RemoveRssCommand(id), future.completer)
                future
            ]
            .setHandler[
                Thread.sleep(1500)
                async.complete
            ]
    }

}
