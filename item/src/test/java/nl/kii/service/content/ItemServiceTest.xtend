package nl.kii.service.content

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.URL
import java.time.Instant
import java.util.Map
import java.util.Properties
import java.util.Random
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import nl.kii.kafka.EmbeddedSingleNodeKafkaCluster
import nl.kii.service.content.ItemServiceTest.ItemCommandHandler
import nl.kii.service.content.command.AddArticle
import nl.kii.service.content.command.AddItemKeywords
import nl.kii.service.content.command.UpdateArticle
import nl.kii.service.content.domain.Article
import nl.kii.service.content.domain.Item
import nl.kii.service.content.domain.TextWeight
import nl.kii.xtend.cqrses.AggregateRoot
import nl.kii.xtend.cqrses.Command
import nl.kii.xtend.cqrses.CommandHandler
import nl.kii.xtend.cqrses.CommandMessage
import nl.kii.xtend.cqrses.DomainEventMessage
import nl.kii.xtend.cqrses.Event
import nl.kii.xtend.cqrses.GenericCommandMessage
import nl.kii.xtend.cqrses.GenericDomainEventMessage
import org.apache.commons.lang3.RandomStringUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.test.TestUtils
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test

import static org.apache.kafka.clients.consumer.ConsumerConfig.*
import static org.apache.kafka.clients.producer.ProducerConfig.*
import static org.apache.kafka.streams.StreamsConfig.*
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
import static org.apache.kafka.streams.StreamsConfig.CLIENT_ID_CONFIG

class ItemServiceTest {

    @ClassRule
    val public static CLUSTER = new EmbeddedSingleNodeKafkaCluster

    val static COMMAND_TOPIC = 'test.commands'
    val static STATE_STORE = 'test.state'
    val static EVENT_TOPIC = 'test.events'

    @BeforeClass
    def static void setUpTopics() {
        CLUSTER.createTopic(COMMAND_TOPIC, 5, 1)
        CLUSTER.createTopic(EVENT_TOPIC, 10, 1)
    }

    @BeforeClass
    def static void setUpJVM() {
        System.setProperty('vertx.logger-delegate-factory-class-name','io.vertx.core.logging.SLF4JLogDelegateFactory')
        System.setProperty('hazelcast.logging.type', 'slf4j')
    }
    
    var KafkaStreams streams
    
    @Before
    def void setUpStreams() {
        // Serializers & Deserializers
        val uuidSerde = new GenericSerde<UUID>
        val commandSerde = new GenericSerde<CommandMessage<? extends Command>>
        val aggregateSerde = new GenericSerde<AggregateRoot>
        val eventSerde = new GenericSerde<DomainEventMessage<? extends Event,? extends AggregateRoot>>
        
        val builder = new KStreamBuilder
        
        // Local Store setup
        val states = Stores.create(STATE_STORE).withKeys(uuidSerde).withValues(aggregateSerde).persistent.build
        builder.addStateStore(states)        
        
        // Stream Topology
        builder
            .stream(uuidSerde, commandSerde, COMMAND_TOPIC)
            .transform([new Transformer<UUID,CommandMessage<? extends Command>,KeyValue<UUID,AggregateRoot>> {
                var ItemCommandHandler commandHandler
                
                override init(ProcessorContext context) {
                    val store = context.getStateStore(STATE_STORE) as KeyValueStore<UUID, AggregateRoot>
                    this.commandHandler = new ItemCommandHandler(store)
                }
                
                override transform(UUID key, CommandMessage<? extends Command> value) {
                    val AggregateRoot aggregate = commandHandler.handle(value.payload)
                    new KeyValue(key, aggregate)
                }
                
                override punctuate(long timestamp) {}
                override close() {}
            }], STATE_STORE)
            .filter[k,v|v !== null]
            .mapValues[aggregate|aggregate.unsaved.map[new GenericDomainEventMessage(aggregate.id, aggregate.class, it)]]
            .flatMapValues[toIterable as Iterable<? extends DomainEventMessage<? extends Event,? extends AggregateRoot>>]
            .to(uuidSerde, eventSerde, EVENT_TOPIC)

        // Streams startup
        val config = new Properties => [
            put(CLIENT_ID_CONFIG, 'testy')
            put(ACKS_CONFIG, 'all')
            put(BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers)
            put(AUTO_OFFSET_RESET_CONFIG, 'earliest')
            put(APPLICATION_ID_CONFIG, 'test')
            put(APPLICATION_SERVER_CONFIG, 'localhost:10000')
        ]
        streams = new KafkaStreams(builder, config)
        streams.cleanUp
        streams.start
    }
    
    @After
    def void tearDownStreams() {
        streams.close
    }
    
    @Test
    def void run() {
        // Generating samples for commands
        val rnd = new Random
        val ids = new AtomicReference(newArrayList)
        val data = (0..100)
                        .map [
                            switch rnd.nextInt(3) {
                                case 0:{
                                    val id = UUID.randomUUID
                                    ids.get.add(id)
                                    new AddArticle(id, RandomStringUtils.randomAlphabetic(10), RandomStringUtils.randomAlphabetic(50), Instant.now, new URL('http://example.com/'+RandomStringUtils.randomAlphabetic(5)))
                                }
                                case 1:{
                                    val id = if (!ids.get.empty) ids.get.get(rnd.nextInt(ids.get.size))
                                    if ( id !== null)
                                        new UpdateArticle(id, RandomStringUtils.randomAlphabetic(10), RandomStringUtils.randomAlphabetic(50))
                                }
                                case 2:{
                                    val id = if (!ids.get.empty) ids.get.get(rnd.nextInt(ids.get.size))
                                    val kw = (0..(rnd.nextInt(5)+1)).map[new TextWeight(RandomStringUtils.randomAlphabetic(5), rnd.nextGaussian)].toList
                                    if ( id !== null)
                                        new AddItemKeywords(id, kw)                                    
                                }
                            }
                        ]
                        .filterNull
                        .toList

        // Setting up kafka producer
        val config = new Properties => [
            put(CLIENT_ID_CONFIG, 'testy')
            put(ACKS_CONFIG, 'all')
            put(BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers)
        ]
        
        val uuidSer = new GenericSerde<UUID>().serializer
        val commandSer = new GenericSerde<CommandMessage<? extends Command>>().serializer

        // Waiting until kafka streams is ready
        TestUtils.waitForCondition([streams.state == KafkaStreams.State.RUNNING], 10000,'Streams not running')

        // Sending commands to kafka
        val producer = new KafkaProducer(config, uuidSer, commandSer)
        data
            .map [cmd|new GenericCommandMessage(cmd.id, cmd, emptyMap, cmd.class.simpleName)]
            .forEach[msg|producer.send(new ProducerRecord(COMMAND_TOPIC, msg.id, msg))]

        // Waiting until the local store is ready
        TestUtils.waitForCondition([!streams.allMetadataForStore(STATE_STORE).empty], 10000,'Store not ready')
        
        // Generating expected aggregate state
        val selected = ids.get.get(rnd.nextInt(ids.get.size))
        val ref = new AtomicReference<Article>
        data.filter[id == selected].forEach[
            switch it {
                AddArticle: { ref.set(new Article(id,title, description, url, published)) }
                UpdateArticle: { ref.get.update(title, description)}
                AddItemKeywords: { ref.get.addKewords(keywords) }
            }
        ]

        // Waiting 2 seconds to kafka stream process the messages
        Thread.sleep(2000)   
        // Getting aggregate from local store      
        val store = streams.store(STATE_STORE, QueryableStoreTypes.<UUID, AggregateRoot>keyValueStore)
        val aggregate = store.get(selected) as Article
        
        Assert.assertNotNull(aggregate)
        Assert.assertEquals(ref.get.id, aggregate.id)
        Assert.assertEquals(ref.get.title, aggregate.title)
        Assert.assertEquals(ref.get.description, aggregate.description)
        Assert.assertEquals(ref.get.published, aggregate.published)
        Assert.assertEquals(ref.get.url, aggregate.url)
        Assert.assertEquals(ref.get.keywords, aggregate.keywords)
    }
    
    
    // Command Handlers & Domain Logic
    @FinalFieldsConstructor
    static class ItemCommandHandler implements CommandHandler {
        
        val KeyValueStore<UUID, AggregateRoot> store
        
        def dispatch handle(AddArticle it) {
            new Article(id, title, description, url, published) => [
                store.put(id, it)
            ]
        }

        def dispatch handle(UpdateArticle it) {
            val article = store.get(id) as Article
            article?.update(title, description) => [
                store.put(id, it)
            ]
        }

        def dispatch handle(AddItemKeywords it) {
            val item = store.get(id) as Item
            item?.addKewords(keywords) => [
                store.put(id, it)
            ]
        }

    }
    
    static class GenericSerde<T> implements Serde<T> {
        
        override close() {}
        
        override configure(Map<String, ?> configs, boolean isKey) {}
        
        override deserializer() {
            new Deserializer<T> {
                override close() {}
                override configure(Map<String, ?> configs, boolean isKey) {}
                override deserialize(String topic, byte[] data) {
                    val bais = new ByteArrayInputStream(data)
                    val in = new ObjectInputStream(bais)
                    in.readObject as T => [
                        in.close
                        bais.close
                    ]
                }
            }
        }
        
        override serializer() {
            new Serializer<T>() {
                override close() {}
                override configure(Map<String, ?> configs, boolean isKey) {}
                override serialize(String topic, Object data) {
                    val baos = new ByteArrayOutputStream
                    val out = new ObjectOutputStream(baos)
                    out.writeObject(data)
                    baos.toByteArray => [
                        out.close
                        baos.close
                    ]
                }
            }
        }
        
    }
}