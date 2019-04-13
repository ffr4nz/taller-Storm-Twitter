package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;


import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQImpl.Connection;

/**
 * Created with IntelliJ IDEA.
 * User: qadeer
 * Date: 06.09.13
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */
public class RabbitMQBolt extends BaseRichBolt {

    private OutputCollector _collector;

    private QueueingConsumer consumer;
    private String uri;
    private Integer timeout;
    private Integer heartbeat;
    private String queue;
    private Channel channel;

    public RabbitMQBolt(){
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

        try {
            uri = "amqp://umhaqtjw:_LRcdKLt_sh5JJcakXbffc7OlOqJAW09@penguin.rmq.cloudamqp.com/umhaqtjw";
            timeout = 30000;
            heartbeat = 30;

            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(uri);

            //Recommended settings
            factory.setRequestedHeartbeat(heartbeat);
            factory.setConnectionTimeout(timeout);


            com.rabbitmq.client.Connection connection = factory.newConnection();
            channel = ((com.rabbitmq.client.Connection) connection).createChannel();

            queue = "utad";     //queue name
            boolean durable = false;    //durable - RabbitMQ will never lose the queue if a crash occurs
            boolean exclusive = false;  //exclusive - if queue only will be used by one connection
            boolean autoDelete = false; //autodelete - queue is deleted when last consumer unsubscribes

            channel.queueDeclare(queue, durable, exclusive, autoDelete, null);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (KeyManagementException | NoSuchAlgorithmException
                | URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {
        try{
            System.out.println("Rabbit");
            String hashtag = tuple.getStringByField("entity");
            System.out.println(hashtag);
            String exchangeName = "";
            String routingKey = "utad";
            /*  Default exchange
                The default exchange is a pre-declared direct exchange with no name, usually referred by the empty
                string "". When you use the default exchange, your message will be delivered to the queue with a
                name equal to the routing key of the message. Every queue is automatically bound to the default
                exchange with a routing key which is the same as the queue name.

                https://www.cloudamqp.com/blog/2015-09-03-part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html
            */
            channel.basicPublish(exchangeName, routingKey, null, hashtag.getBytes());
            System.out.println(" [x] Sent '" + hashtag + "'");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // Confirm that this tuple has been treated.
        _collector.ack(tuple);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        super.cleanup();
    }
}
