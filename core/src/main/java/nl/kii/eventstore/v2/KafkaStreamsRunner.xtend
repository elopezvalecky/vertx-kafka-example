package nl.kii.eventstore.v2

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Verticle
import io.vertx.core.impl.CompositeFutureImpl
import io.vertx.core.shareddata.Shareable
import java.beans.Statement
import java.util.ArrayList
import java.util.List
import java.util.Properties
import java.util.UUID
import java.util.stream.Collectors
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.processor.StreamPartitioner
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

import static extension io.vertx.core.logging.LoggerFactory.getLogger

@FinalFieldsConstructor
class KafkaStreamsRunner extends AbstractVerticle {

    val logger = class.logger

    val UUID instanceId = UUID.randomUUID
    val KStreamBuilder builder = new KStreamBuilder
    
    val Properties config
    val List<Verticle> verticles
    
    var KafkaStreams streams
        
    override start(Future<Void> startFuture) throws Exception {
        Future.succeededFuture
            .compose[
                <Void>execute [
                    logger.info('Starting KafkaStreams with application.server set to {}', instanceId.toString)
                    config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, '''«instanceId»:10000'''.toString)
                    initializeKafkaStreams
                    vertx.sharedData.getLocalMap('streams').put('metadata', new StreamsMetadata(streams))
                    complete()            
                ]
            ]
            .compose[CompositeFutureImpl.all(deploy(new ArrayList(this.verticles))).<Void>map[null]]
            .setHandler(startFuture.completer)
    }
    
    override stop(Future<Void> stopFuture) throws Exception {
        vertx.executeBlocking([
            logger.info('Shutting down KafkaStreams')
            streams.close
            complete()
        ], stopFuture.completer)
    }
 
    def private initializeKafkaStreams() {
        streams = new KafkaStreams(this.builder, this.config)
        
        streams.setStateListener [newState, oldState|
            logger.debug('State change in KafkaStreams recorded: oldstate={}, newstate={}', oldState, newState)
        ]
        
        streams.cleanUp
        streams.start
    }
    
    def private <V extends Verticle> List<Future<String>> deploy(List<V> verticles) {
        verticles
            .stream
            .map [
                val future = Future.future
                
                new Statement(it, 'setInstanceId', #[this.instanceId] ).execute
                new Statement(it, 'setBuilder', #[this.builder]).execute
                new Statement(it, 'setStreams', #[this.streams]).execute
                
                vertx.deployVerticle(it, future.completer)
                future
            ]
            .collect(Collectors.toList)
    }
    
    def private <T> execute(Handler<Future<T>> fn) {
        val future = Future.<T>future
        vertx.executeBlocking(fn, future.completer)
        future
    }

    @FinalFieldsConstructor
    static class StreamsMetadata implements Shareable {

        val KafkaStreams streams

        def allMetadata() {
            streams.allMetadata()
        }
    
        def allMetadataForStore(String storeName) {
            streams.allMetadataForStore(storeName)
        }
    
        def <K> metadataForKey(String storeName, K key, Serializer<K> keySerializer) {
            streams.metadataForKey(storeName, key, keySerializer)
        }
    
        def <K> metadataForKey(String storeName, K key, StreamPartitioner<? super K, ?> partitioner) {
            streams.metadataForKey(storeName, key, partitioner)
        }
    
    }
    
}