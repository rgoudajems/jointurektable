package testjoin.com.test.kafka;
 

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
 
public class JointureTables {
 
    public static void main(final String[] args) throws Exception {
    	  final String userRegionTopic = "user-region-topic";
    	  final String userLastLoginTopic = "user-last-login-topic";
    	final String outputTopic = "output-topic";
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "table-table-join-lambda-integration-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        // For didactic reasons: disable record caching so we can observe every individual update record being sent downstream
        //config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        //config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        //config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //// Use a temporary directory for storing state, which will be automatically removed after the test.
       // config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        

    
 
      /*  StreamsBuilder builder = new StreamsBuilder();  
        //KTable<String, String> adressestable = builder.table("adresses");
        JsonSerde<Adresse> adresseSerde = new JsonSerde<>(Adresse.class);
        KTable<String, Adresse> adressestable = builder.table("adresses", Consumed.with(Serdes.String(), adresseSerde));
        JsonSerde<Contact> contactSerde = new JsonSerde<>(Contact.class);
        KTable<String, Contact> contacttable = builder.table("contact", Consumed.with(Serdes.String(), contactSerde));        
        String storeName = "joined-store";
        adressestable.join(contacttable,
        		(adresseValue, contactValue) -> adresseValue + "/" + contactValue,
        		Materialized.as(storeName)).
        		toStream().
        		to( "output-topic" , Produced.with(Serdes.String(),))
        		arg1) */
        
        // Input: Region per user (multiple records allowed per user).
        List<KeyValue<String, String>> userRegionRecords = Arrays.asList(
            new KeyValue<>("alice", "asia"),
            new KeyValue<>("bob", "europe"),
            new KeyValue<>("alice", "europe"),
            new KeyValue<>("charlie", "europe"),
            new KeyValue<>("bob", "asia")
        );

        // Input 2: Timestamp of last login per user (multiple records allowed per user)
        List<KeyValue<String, Long>> userLastLoginRecords = Arrays.asList(
            new KeyValue<>("alice", 1485500000L),
            new KeyValue<>("bob", 1485520000L),
            new KeyValue<>("alice", 1485530000L),
            new KeyValue<>("bob", 1485560000L)
            
        );

        List<KeyValue<String, String>> expectedResults = Arrays.asList(
            new KeyValue<>("alice", "europe/1485500000"),
            new KeyValue<>("bob", "asia/1485520000"),
            new KeyValue<>("alice", "europe/1485530000"),
            new KeyValue<>("bob", "asia/1485560000")
        );

        List<KeyValue<String, String>> expectedResultsForJoinStateStore = Arrays.asList(
            new KeyValue<>("alice", "europe/1485530000"),
            new KeyValue<>("bob", "asia/1485560000")
        );
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
    
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> userRegions = builder.table(userRegionTopic);
        KTable<String, Long> userLastLogins = builder.table(userLastLoginTopic, Consumed.with(stringSerde, longSerde));

        String storeName = "joined-store";
        userRegions.join(userLastLogins,
            (regionValue, lastLoginValue) -> regionValue + "/" + lastLoginValue,
            Materialized.as(storeName))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));


        KafkaStreams streams = new KafkaStreams(builder.build(), config );
        streams.start();
    
        //
        // Step 2: Publish user regions.
        //
        Properties regionsProducerConfig = new Properties();        
        regionsProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        regionsProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        regionsProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        regionsProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        regionsProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
       // IntegrationTestUtils.produceKeyValuesSynchronously(userRegionTopic, userRegionRecords, regionsProducerConfig);

        //
        // Step 3: Publish user's last login timestamps.
        //
        Properties lastLoginProducerConfig = new Properties();
        lastLoginProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        lastLoginProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        lastLoginProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        lastLoginProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        lastLoginProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    //   IntegrationTestUtils.produceKeyValuesSynchronously(userLastLoginTopic, userLastLoginRecords, lastLoginProducerConfig);

        //
        // Step 4: Verify the application's output data.
        //
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "stream-stream-join-lambda-integration-test-standard-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<KeyValue<String, String>> actualResults = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig,
            outputTopic, expectedResults.size());

        // Verify the (local) state store of the joined table.
        // For a comprehensive demonstration of interactive queries please refer to KafkaMusicExample.
        ReadOnlyKeyValueStore<String, String> readOnlyKeyValueStore =
            streams.store(storeName, QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, String> keyValueIterator = readOnlyKeyValueStore.all();
        assertThat(keyValueIterator).containsExactlyElementsOf(expectedResultsForJoinStateStore);

        streams.close();
        assertThat(actualResults).containsExactlyElementsOf(expectedResults);
        
         
 
}
/*
 * // Création d'un KStream (flux) à partir du topic "achats"
KStream<String, ProduitBrut> achats = builder.stream(Serdes.String(), produitBrutSerde, "achats");

// Création d'une KTable (table) à partir du topic "referentiel"
KTable<String, Referentiel> referentiel = builder.table(Serdes.String(), referentielSerde, "referentiel");
KStream<String, ProduitEnrichi> enriched = achats
    // Re-partitionnement du flux avec la nouvelle clé qui nous permettra de faire une jointure
    .map((k, v) -> new KeyValue<>(v.getId().toString(), v))
    // Jointure du flux d'achats avec le référentiel
    .leftJoin(referentiel, (achat, ref) -> {
        if (ref == null) return new ProduitEnrichi(achat.getId(), "REF INCONNUE", achat.getPrice());
        else return new ProduitEnrichi(achat.getId(), ref.getName(), achat.getPrice());
    });

// On publie le flux dans un topic "achats-enrichis"
enriched.to(Serdes.String(), produitEnrichiSerde, "achats-enrichis");

// Enfin, on démarre l'application
KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
streams.start();*/
}