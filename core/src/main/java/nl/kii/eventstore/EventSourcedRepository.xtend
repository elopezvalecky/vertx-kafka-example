package nl.kii.eventstore

import io.vertx.core.AsyncResult
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus
import io.vertx.core.json.JsonObject
import io.vertx.core.net.impl.ServerID
import io.vertx.core.shareddata.AsyncMap
import io.vertx.core.shareddata.SharedData
import io.vertx.serviceproxy.ServiceBinder
import io.vertx.serviceproxy.ServiceProxyBuilder
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.lang.reflect.AccessibleObject
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Member
import java.lang.reflect.Modifier
import java.security.AccessController
import java.security.PrivilegedAction
import java.util.ArrayList
import java.util.Iterator
import java.util.Map
import java.util.UUID
import nl.kii.eventsourcing.AggregateRoot
import nl.kii.eventsourcing.DomainEventMessage
import nl.kii.eventsourcing.Event
import nl.kii.eventsourcing.GenericDomainEventMessage
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.Stores
import org.eclipse.xtend.lib.annotations.Accessors

import static extension io.vertx.core.logging.LoggerFactory.getLogger
import static extension java.util.UUID.fromString

class EventSourcedRepository implements Repository, Closeable {

    val private static SHARED_DATA_KEY = 'repository'
    val private static INTERMEDIATE_EVENTS_STATE_STORE = 'events-per-aggregate-store'
    val private static STATE_STORE_ADDRESS_PREFIX = 'state.store/'

    val logger = class.logger

    @Accessors(PUBLIC_GETTER)
    val UUID id
    
    val Vertx vertx
    val KafkaStreams streams
    val KafkaProducer<String, DomainEventMessage<? extends Event>> producer
    val String topic
    val String stateStore
    
    val ServiceBinder binder
    val MessageConsumer<JsonObject> proxy
    
    new(Vertx vertx, String topic, String stateStore, Map<String, Object> properties) {
        this.vertx = vertx
        this.topic = topic
        this.stateStore = stateStore
        
        this.binder = new ServiceBinder(vertx)

        this.id = (vertx.orCreateContext.deploymentID?.fromString)?:UUID.randomUUID
        if (vertx.clustered) {
            val localStateStore = new StateStore {
                override load(Request request, Handler<AsyncResult<Buffer>> result) {
                    val data = EventSourcedRepository.this.localLoad(request.id, request.clazz).serialize.map[Buffer.buffer(it)]
                    result.handle(data)
                }
            }
            
            this.proxy = binder.setAddress(STATE_STORE_ADDRESS_PREFIX + EventSourcedRepository.this.id).register(StateStore, localStateStore)

            val serverIDField  = ClusteredEventBus.getDeclaredField('serverID') => [
                accessible = true
            ]
            val node = serverIDField.get(vertx.eventBus) as ServerID
            properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, '''«node.host»:«node.port»'''.toString)
        
            vertx.sharedData.getClusterWideMap(SHARED_DATA_KEY) [
                if (failed) throw cause
                result.put('''«node.host»:«node.port»''', this.id) [
                    if (failed) throw cause
                ]
            ]
        } else {
            this.proxy = null
        }

        val config = new StreamsConfig(properties)
        this.producer = createKafkaProducer(config)
        this.streams = createKafkaStreams(config)
        this.streams.cleanUp
        this.streams.start
    }

    override <AR extends AggregateRoot> save(AR aggregate, Map<String, ?> metadata) {
        val futures = newArrayList
        aggregate
            .unsaved
            .map [new GenericDomainEventMessage(aggregate.id, aggregate.class.name, it, metadata?:emptyMap)]
            .forEach [
                val recordFuture = Future.<Void>future
                futures.add(recordFuture)
                val ProducerRecord<String, DomainEventMessage<? extends Event>> record = new ProducerRecord(this.topic, aggregate.id.toString, it)
                this.producer.send(record) [ rmd, ex |
                    if (ex !== null) {
                        recordFuture.fail(ex)
                        logger.error('Send failed for record {}', record, ex)
                    } else {
                        recordFuture.complete()
                    }
            ]
        ]
        this.producer.flush
        Future.<Void>future => [ f |
            if (futures.empty) {
                f.complete()
            } else {
                CompositeFuture.all(futures).setHandler [
                    if (failed) {
                        f.fail(cause)
                    } else {
                        f.complete()
                    }
                ]
            }
        ]
    }

    override <AR extends AggregateRoot> load(UUID aggregateId, Class<AR> aggregateClass) {
        val future = Future.future
        vertx.executeBlocking([
            val metadata = this.streams.metadataForKey(this.stateStore, aggregateId.toString, new StringSerializer)
            if (metadata === null || !vertx.clustered) {
                localLoad(aggregateId, aggregateClass).setHandler(completer)
            } else {
                Future.succeededFuture('''«metadata.host»:«metadata.port»''')
                    .compose[hostInfo| vertx.sharedData.getClusterWideMap(SHARED_DATA_KEY).compose[get(hostInfo)] ]
                    .map[ it as UUID]
                    .compose[nodeId|
                        if (this.id.equals(nodeId)) {
                            localLoad(aggregateId, aggregateClass)
                        } else {
                            remoteLoad(nodeId, aggregateId, aggregateClass)
                        }
                    ]
                    .setHandler(completer)
            }
            
        ], future.completer)
        future
    }
    
    override close() throws IOException {
        if (this.proxy !== null) binder.unregister(this.proxy)
        this.streams.close
        this.producer.close
    }

    def isStoreReady() {
        !this.streams.allMetadataForStore(this.stateStore).empty
    }
    
    def private createKafkaProducer(StreamsConfig config) {

        val producerConfig = config.getProducerConfigs(this.id.toString)
        
        new KafkaProducer(producerConfig, new StringSerializer, new DomainEventMessageSerializer)
    }
    
    def private createKafkaStreams(StreamsConfig config) {

        val aggregateSerde = Serdes.serdeFrom(new AggregateRootSerializer, new AggregateRootDeserializer)
        val messageSerde = Serdes.serdeFrom(new DomainEventMessageSerializer, new DomainEventMessageDeserializer)
        val eventsSerde = Serdes.serdeFrom(new JavaSerializer<ArrayList<Event>>, new JavaDeserializer<ArrayList<Event>>)
        val pairSerde = Serdes.serdeFrom(new JavaSerializer<Pair<String,String>>, new JavaDeserializer<Pair<String,String>>)

        val builder = new KStreamBuilder
        val aggregateStore = Stores.create(this.stateStore)
                                  .withKeys(Serdes.String)
                                  .withValues(aggregateSerde)
                                  .persistent
                                  .build
        builder.addStateStore(aggregateStore)

        val eventsPerAggregateStore = Stores.create(INTERMEDIATE_EVENTS_STATE_STORE)
                                          .withKeys(pairSerde)
                                          .withValues(eventsSerde)
                                          .persistent
                                          .build
            
        val events = builder.stream(Serdes.String, messageSerde, this.topic)

        events
            .selectKey[k,v|v.aggregateType -> k]
            .groupByKey
            .aggregate([<Event>newArrayList],[$2=>[add($1.payload)]],eventsPerAggregateStore)
            .toStream
            .transform([new AggregateRootTransformer(aggregateStore.name)],aggregateStore.name)

        new KafkaStreams(builder, config)
    }
    
    def private <AR extends AggregateRoot> localLoad(UUID aggregateId, Class<AR> aggregateClass) {
        println('Local')
        val future = Future.future
        this.vertx.executeBlocking([
            val store = this.streams.store(this.stateStore, QueryableStoreTypes.<String, AR>keyValueStore)
            try {
                complete(aggregateClass.cast(store.get(aggregateId.toString)))
            } catch (Exception e) {
                fail(e)
            }
        ],future.completer)
        future
    }
    
    def private <AR extends AggregateRoot> remoteLoad(UUID nodeId, UUID aggregateId, Class<AR> aggregateClass) {
        println('Remote')
        val request = new Request(aggregateId, aggregateClass)
        val future = Future.future
        val proxy = new ServiceProxyBuilder(this.vertx).setAddress(STATE_STORE_ADDRESS_PREFIX + nodeId).build(StateStore)
        proxy.load(request, future.completer)
        future.deserialize(aggregateClass)
    }
    
    def private getClusterWideMap(SharedData sharedData, String name) {
        val future = Future.future
        sharedData.getClusterWideMap(name, future.completer)
        future
    }
    
    def private get(AsyncMap<Object,Object> asyncMap, String name) {
        val future = Future.future
        asyncMap.get(name, future.completer)
        future
    }

    def private <AR extends AggregateRoot> serialize(Future<AR> future) {
        future.map [
            val baos = new ByteArrayOutputStream
            val out = new ObjectOutputStream(baos)
            out.writeObject(it)
            baos.toByteArray => [
                out.close
                baos.close
            ]
        ]
    }
    
    def private <AR extends AggregateRoot> deserialize(Future<Buffer> future, Class<AR> clazz) {
        future.map [
            val bais = new ByteArrayInputStream(bytes)
            val in = new ObjectInputStream(bais)
            clazz.cast(in.readObject) => [
                in.close
                bais.close
            ]            
        ]        
    }
    
}

class Helper {

    def static <AR extends AggregateRoot> getInstance(Class<AR> aggregateClass, Iterator<Event> events) {
        try {
            val constructor = if (events === null) {
                    aggregateClass.getDeclaredConstructor
                } else {
                    aggregateClass.getDeclaredConstructor(Iterator)
                }
            if (!isAccessible(constructor)) {
                AccessController.doPrivileged(new PrivilegedAction {
                    override run() {
                        constructor.setAccessible(true)
                        Void
                    }
                })
            }
            if (events === null) {
                constructor.newInstance
            } else {
                constructor. newInstance(events)
            }
        } catch (InstantiationException e) {
            throw new UnsupportedOperationException('''The aggregate [«aggregateClass.simpleName»] does not have a suitable no-arg constructor.''', e)
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException('''The aggregate no-arg constructor of the aggregate [«aggregateClass.simpleName»] is not accessible. Please ensure that the constructor is public or that the Security Manager allows access through reflection.''', e)
        } catch (InvocationTargetException e) {
            throw new UnsupportedOperationException('''The no-arg constructor of [«aggregateClass.simpleName»] threw an exception on invocation.''', e)
        }
    }

    def static isAccessible(AccessibleObject member) {
        return member.isAccessible() || (Member.isInstance(member) && isNonFinalPublicMember(member as Member))
    }

    def static isNonFinalPublicMember(Member member) {
        return (Modifier.isPublic(member.getModifiers()) &&
            Modifier.isPublic(member.getDeclaringClass().getModifiers()) &&
            !Modifier.isFinal(member.getModifiers()))
    }
    
}