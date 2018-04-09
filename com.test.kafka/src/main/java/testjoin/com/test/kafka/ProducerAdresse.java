package testjoin.com.test.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Properties;
import java.util.Scanner;

/**
 * Created by sunilpatil on 12/25/16.
 */
public class ProducerAdresse {
    private static Scanner in;
    public static void main(String[] argv)throws Exception {
      /*C1 , PARIS  if (argv.length != 1) {
            System.err.println("Please specify 1 parameters ");
            System.exit(-1);
        }*/
        String topicName = "adresse";
        in = new Scanner(System.in);
        System.out.println("Enter message(type exit to quit)");

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,JsonSerializer.class);
        configProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        configProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 10);
        configProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);

        ObjectMapper objectMapper = new ObjectMapper();

        String line = in.nextLine();
        while(!line.equals("exit")) {
            Adresse adresse = new Adresse();
            adresse.parseString(line);
           
            JsonNode  jsonNode = objectMapper.valueToTree(adresse);
            ProducerRecord<String,JsonNode> rec = new ProducerRecord(topicName, adresse.getId(),jsonNode);
            producer.send(rec);
            line = in.nextLine();
        }
        in.close();
        producer.close();
    }
}