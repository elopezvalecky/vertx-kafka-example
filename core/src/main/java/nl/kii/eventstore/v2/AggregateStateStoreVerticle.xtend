package nl.kii.eventstore.v2

import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import io.vertx.serviceproxy.ServiceBinder
import io.vertx.serviceproxy.ServiceProxyBuilder
import java.beans.Statement
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.ArrayList
import java.util.UUID
import javax.annotation.Resource
import nl.kii.eventsourcing.AggregateRoot
import nl.kii.eventsourcing.Event
import nl.kii.eventstore.AggregateRootDeserializer
import nl.kii.eventstore.AggregateRootSerializer
import nl.kii.eventstore.AggregateRootTransformer
import nl.kii.eventstore.DomainEventMessageDeserializer
import nl.kii.eventstore.DomainEventMessageSerializer
import nl.kii.eventstore.JavaDeserializer
import nl.kii.eventstore.JavaSerializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.Stores
import org.eclipse.xtend.lib.annotations.Accessors
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
class AggregateStateStoreVerticle extends AbstractVerticle {
    
    val String topic
    val String store
    val Repository repository
    
    var ServiceBinder binder
    var MessageConsumer<JsonObject> proxy

    @Resource
    @Accessors(PUBLIC_SETTER) 
    var UUID instanceId
    
    @Resource
    @Accessors(PUBLIC_SETTER) 
    var KafkaStreams streams
    
    @Resource
    @Accessors(PUBLIC_SETTER) 
    var KStreamBuilder builder
    
    override start(Future<Void> startFuture) throws Exception {
        // Initialize EventBus Consumer for internode communication.
        val localStore = new LocalStore {            
            override <AR extends AggregateRoot> load(UUID id, Class<AR> type, Handler<AsyncResult<Buffer>> result) {
                val future = Future.future
                vertx.executeBlocking([
                    val store = streams.store(store, QueryableStoreTypes.<String, AggregateRoot>keyValueStore)
                    try {
                        complete(type.cast(store.get(id.toString)))
                    } catch (Exception e) {
                        fail(e)
                    }
                ],future.completer)
                result.handle(future.serialize.map[Buffer.buffer(it)])
            }
        }
        this.binder = new ServiceBinder(vertx)
        this.proxy = binder.setAddress('store:aggregate#' + instanceId).register(LocalStore, localStore)
        
        // Injecting state store into the repository
        try {
            new Statement(repository, 'setStateStore', #[stateStore]).execute
        } catch (Throwable t) {
            startFuture.fail(t)
            return
        }

        // Setting up the stream to reconstruct the state of the aggregate from events.
        vertx.executeBlocking([
            val aggregateSerde = Serdes.serdeFrom(new AggregateRootSerializer, new AggregateRootDeserializer)
            val messageSerde = Serdes.serdeFrom(new DomainEventMessageSerializer, new DomainEventMessageDeserializer)
            val eventsSerde = Serdes.serdeFrom(new JavaSerializer<ArrayList<Event>>, new JavaDeserializer<ArrayList<Event>>)
            val pairSerde = Serdes.serdeFrom(new JavaSerializer<Pair<String,String>>, new JavaDeserializer<Pair<String,String>>)
    
            val eventsPerAggregateStore = Stores.create('events-per-aggregate')
                                              .withKeys(pairSerde)
                                              .withValues(eventsSerde)
                                              .persistent
                                              .build
    
            val aggregateStore = Stores.create(store)
                                      .withKeys(Serdes.String)
                                      .withValues(aggregateSerde)
                                      .persistent
                                      .build
                
            builder
                .addStateStore(aggregateStore)
    
            builder
                .stream(Serdes.String, messageSerde, topic)
                .selectKey[k,v|v.aggregateType -> k]
                .groupByKey
                .aggregate([<Event>newArrayList],[$2=>[add($1.payload)]],eventsPerAggregateStore)
                .toStream
                .transform([new AggregateRootTransformer(aggregateStore.name)],aggregateStore.name)        
        ], startFuture.completer)
    }
    
    override stop(Future<Void> stopFuture) throws Exception {
        if (this.proxy !== null)
            binder.unregister(this.proxy)
    }
    
    def private stateStore() {
        new StateStore {
            override <AR extends AggregateRoot> load(UUID id, Class<AR> type) {
                val future = Future.future
                vertx.executeBlocking([
                    val metadata = streams.metadataForKey(store, id.toString, new StringSerializer)
                    if (metadata === null || !vertx.clustered) {
                        localLoad(id, type).setHandler(completer)
                    } else {
                        Future.succeededFuture(metadata.host)
                            .compose[node|
                                if (instanceId.equals(UUID.fromString(node))) {
                                    localLoad(id, type)
                                } else {
                                    remoteLoad(node, id, type)
                                }
                            ]
                            .setHandler(completer)
                    }
                ], future.completer)
                future
            }

            def private <AR extends AggregateRoot> localLoad(UUID aggregateId, Class<AR> aggregateClass) {
                println('Local')
                val future = Future.future
                vertx.executeBlocking([
                    val store = streams.store(store, QueryableStoreTypes.<String, AR>keyValueStore)
                    try {
                        complete(aggregateClass.cast(store.get(aggregateId.toString)))
                    } catch (Exception e) {
                        fail(e)
                    }
                ],future.completer)
                future
            }
            
            def private <AR extends AggregateRoot> remoteLoad(String node, UUID aggregateId, Class<AR> aggregateType) {
                println('Remote')
                val future = Future.future
                val proxy = new ServiceProxyBuilder(vertx).setAddress('store:aggregate#' + node).build(LocalStore)
                proxy.load(aggregateId, aggregateType, future.completer)
                future.deserialize(aggregateType)
            }            
        }
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