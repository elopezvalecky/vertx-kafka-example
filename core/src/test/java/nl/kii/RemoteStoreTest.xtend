package nl.kii;

import com.fasterxml.jackson.databind.SerializationFeature
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus
import io.vertx.core.json.Json
import io.vertx.core.net.impl.ServerID
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import java.net.URL
import java.util.Map
import java.util.Properties
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier
import nl.kii.kafka.EmbeddedSingleNodeKafkaCluster
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.test.TestUtils
import org.eclipse.xtend.lib.annotations.Accessors
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

import static extension io.vertx.core.logging.LoggerFactory.getLogger

@RunWith(VertxUnitRunner)
class RemoteStoreTest {

    val logger = class.logger
    
    val static TOPIC = 'test-events'

    @ClassRule
    val public static CLUSTER = new EmbeddedSingleNodeKafkaCluster

    @BeforeClass
    def static void setUpJVM() {
        System.setProperty('vertx.logger-delegate-factory-class-name','io.vertx.core.logging.SLF4JLogDelegateFactory')
    }

    @BeforeClass
    def static void setUpTopics() {
        CLUSTER.createTopic(TOPIC, 2, 1)
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

    @Rule public val rule0 = new RunTestOnContext(supplier)
    @Rule public val rule1 = new RunTestOnContext(supplier)
    @Rule public val rule2 = new RunTestOnContext(supplier)
    
    @Test
    def void distributed(TestContext context) {
        val async = context.async
        
        val eventTypes = #['Added','Updated','Removed']
        val entityTypes = #['Rss','Article']
        
        val data = (1..10).map [idx|entityTypes.map [entity|eventTypes.map [event|idx -> '''«entity»$«event»''']]].flatten.flatten
        
        val pProps = new Properties => [
            put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers)            
            put(ProducerConfig.ACKS_CONFIG, 'all')
        ]
        
        val vertx0 = rule0.vertx
        
        val sProps = new Properties => [
            putAll(pProps)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 'earliest')
            put(StreamsConfig.APPLICATION_ID_CONFIG, 'test')
            put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 500)
        ]

        val future1 = Future.future
        val vertx1 = rule1.vertx
        vertx1.executeBlocking([initStream(vertx1, sProps)], future1.completer)

        val future2 = Future.future
        val vertx2 = rule2.vertx
        vertx2.executeBlocking([initStream(vertx2, sProps)], future2.completer)

        CompositeFuture.all(future1, future2)
            .compose[
                val future = Future.<Void>future                
                vertx0.executeBlocking([root|
                    val producer = new KafkaProducer(pProps, new IntegerSerializer, new StringSerializer)
                    val futures = newArrayList
                    data.forEach[
                        val record = new ProducerRecord(TOPIC, key, value)
                        producer.send(record) [ rmd, ex |
                            if (ex !== null) futures.add(Future.failedFuture(ex))
                            else futures.add(Future.succeededFuture)
                        ]
                    ]
                    producer.flush
                    producer.close
                    CompositeFuture.all(futures).setHandler [
                        if (failed) root.fail(cause)
                        else root.complete()
                    ]
                ], future.completer)
                future
            ]
            .compose[it|
                val future = Future.future
                vertx0.eventBus.send('rss', data.get(3).key, future.completer)
                future
            ]
            .map[body]        
            .setHandler [
                if (failed) {
                    context.fail(cause)
                    return
                }
                context.assertEquals(6L, result)
                async.complete
            ]
        //async.await(10000)
    }

    def initStream(Future<Void> future, Vertx vertx, Properties props) {
        try {
            val node = vertx.getServerID
            
            val sProps = new Properties => [
                putAll(props)
                put(StreamsConfig.CLIENT_ID_CONFIG, UUID.randomUUID.toString)
                put(StreamsConfig.APPLICATION_SERVER_CONFIG, node.toString)
                put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Integer.class)
                put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.class)
            ]
    
            val builder = new KStreamBuilder
    
            val eventStream = builder.stream(Serdes.Integer, Serdes.String, TOPIC)
            
            // RSS Store
            val storeName = 'test-store'
            eventStream.groupByKey.count(storeName)
            
            val streams = new KafkaStreams(builder, new StreamsConfig(sProps));
            
            streams.cleanUp
            streams.start
            
            TestUtils.waitForCondition([!streams.allMetadataForStore(storeName).empty], 10000, 'StreamsMetadata should be available')
            
            vertx.eventBus.consumer('''«node.host»:«node.port»/rss''') [
                val store = streams.store(storeName, QueryableStoreTypes.<Integer, Long>keyValueStore)
                reply(store.get(body))
                logger.info('''Node(«node») replied''')
            ]
            vertx.eventBus.consumer('''rss''') [ msg|
                val metadata = streams.metadataForKey(storeName, msg.body, new IntegerSerializer)
                vertx.eventBus.send('''«metadata.host»:«metadata.port»/rss''', msg.body) [
                    if (succeeded) 
                        msg.reply(result.body)
                    else 
                        msg.fail(0, cause.message)
                ]
                logger.info('''Node(«node») got request''')
            ]
            future.complete()
        } catch (Exception e) {
            future.fail(e)
        }
    }
    
    @Accessors(PUBLIC_GETTER)
    static class RssAdded {
        var UUID id
        var String title
        var URL url
        
        new() {}
        
        new(UUID id, String title, URL url) {
            this.id = id
            this.title = title
            this.url = url
        }
    }
    
    @Accessors(PUBLIC_GETTER)
    static class ArticleAdded {
        var UUID id
        var String title
        var String description
        var URL url

        new() {}
        
        new(UUID id, String title, String description, URL url) {
            this.id = id
            this.title = title
            this.description = description
            this.url = url
        }
    }

    static class JavaSerializer<T> implements Serializer<T> {        
        override close() {}
        override configure(Map<String, ?> configs, boolean isKey) {}
        override serialize(String topic, T data) {
            Json.mapper.writeValueAsBytes(data)
        }
    } 

    static class JavaDeserializer<T> implements Deserializer<T> {
        val Class<T> clazz
        new(Class<T> clazz) {
            this.clazz = clazz
        }
        override close() {}
        override configure(Map<String, ?> configs, boolean isKey) {}
        override deserialize(String topic, byte[] data) {
            Json.mapper.readValue(data, clazz)
        }
    }

    private def getServerID(Vertx vertx) {
        val serverIDField  = ClusteredEventBus.getDeclaredField('serverID') => [
            accessible = true
        ]
        serverIDField.get(vertx.eventBus) as ServerID
    }

}
