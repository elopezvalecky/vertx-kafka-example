package nl.kii.service.content

import io.vertx.core.Future
import java.util.UUID
import nl.kii.io.vertx.microservice.common.BaseMicroserviceVerticle
import nl.kii.service.content.ItemServiceTest.GenericSerde
import nl.kii.service.content.command.AddArticle
import nl.kii.service.content.command.AddItemKeywords
import nl.kii.service.content.command.UpdateArticle
import nl.kii.service.content.domain.Article
import nl.kii.service.content.domain.Item
import nl.kii.xtend.cqrses.AggregateRoot
import nl.kii.xtend.cqrses.Command
import nl.kii.xtend.cqrses.CommandMessage
import nl.kii.xtend.cqrses.DomainEventMessage
import nl.kii.xtend.cqrses.Event
import nl.kii.xtend.cqrses.GenericDomainEventMessage
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

import static extension io.vertx.core.logging.LoggerFactory.getLogger
import io.vertx.core.AbstractVerticle

class ItemVerticle extends AbstractVerticle {

    val static STATE_STORE = 'content-service-item-states'
    val static EVENT_TOPIC = 'content-service-item-events'
    val static COMMAND_TOPIC = 'content-service-item-commands'

    val logger = class.logger
    
    var KafkaStreams streams
    
    override start(Future<Void> startFuture) throws Exception {
        vertx.executeBlocking([
            super.start
            logger.info('Starting <Item> service')
    
            val stateStore = config.getString('service.state.store', STATE_STORE)
            val eventTopic = config.getString('service.event.topic', EVENT_TOPIC)
            val commandTopic = config.getString('service.command.topic', COMMAND_TOPIC)
            val kafkaConfig = config.getJsonObject('kafka')

            val uuidSerde = new GenericSerde<UUID>
            val commandSerde = new GenericSerde<CommandMessage<? extends Command>>
            val aggregateSerde = new GenericSerde<AggregateRoot>
            val eventSerde = new GenericSerde<DomainEventMessage<? extends Event,? extends AggregateRoot>>()

            // KafkaStreams Topology
            val builder = new KStreamBuilder

            // KafkaStreams LocalStore
            val states = Stores.create(STATE_STORE).withKeys(uuidSerde).withValues(aggregateSerde).persistent.build
            builder.addStateStore(states)        
            
            // Stream processing for commands
            builder
                .stream(uuidSerde, commandSerde, commandTopic)
                .transform([new CommandHandler(stateStore)], stateStore)
                .filter[k,v|v !== null]
                .mapValues[aggregate|aggregate.unsaved.map[new GenericDomainEventMessage(aggregate.id, aggregate.class, it)]]
                .flatMapValues[toIterable as Iterable<? extends DomainEventMessage<? extends Event,? extends AggregateRoot>>]
                .to(uuidSerde, eventSerde, eventTopic)

            logger.info('Starting up KafkaStreams')
            streams = new KafkaStreams(builder, new StreamsConfig(kafkaConfig.map))
            streams.setStateListener [newState, oldState|
                logger.debug('State change in KafkaStreams recorded: oldstate={}, newstate={}', oldState, newState)
            ]
            streams.start
            complete()
        ], startFuture.completer)
    }
    
    override stop(Future<Void> stopFuture) throws Exception {
        vertx.executeBlocking([
            logger.info('Shutting down KafkaStreams')
        ], stopFuture.completer)
    }

    @FinalFieldsConstructor
    static abstract class KafkaCommandHandler implements Transformer<UUID,CommandMessage<? extends Command>,KeyValue<UUID,AggregateRoot>> {

        val protected String storeName
        var protected KeyValueStore<UUID, AggregateRoot> store
        
        override init(ProcessorContext context) {
            store = context.getStateStore(storeName) as KeyValueStore<UUID, AggregateRoot>
        }
        
        override transform(UUID key, CommandMessage<? extends Command> value) {
            new KeyValue(key, process(value.payload))
        }
        
        override punctuate(long timestamp) {}
        override close() {}

        def AggregateRoot process(Command command)        
    }
    
    @FinalFieldsConstructor
    static class CommandHandler extends KafkaCommandHandler {
        
        def dispatch process(AddArticle it) {
            new Article(id, title, description, url, published) => [
                store.put(id, it)
            ]
        }

        def dispatch process(UpdateArticle it) {
            val article = store.get(id) as Article
            article?.update(title, description) => [
                store.put(id, it)
            ]
        }

        def dispatch process(AddItemKeywords it) {
            val item = store.get(id) as Item
            item?.addKewords(keywords) => [
                store.put(id, it)
            ]
        }                    
    }

}