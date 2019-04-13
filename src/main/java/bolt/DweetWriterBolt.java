package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.net.URLEncoder;

/**
 * Created with IntelliJ IDEA.
 * User: qadeer
 * Date: 06.09.13
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */
public class DweetWriterBolt extends BaseRichBolt {

    private OutputCollector _collector;

    public DweetWriterBolt(){
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("Dweet");
        String hashtag = tuple.getStringByField("entity");
        System.out.println(hashtag);
        try {
            String address = "https://dweet.io/dweet/for/utad_thing?hashtag="+URLEncoder.encode(hashtag, "UTF-8");
            URL url = new URL(address);
            HttpURLConnection connection = (HttpURLConnection)url.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            String json = "";
            while ((inputLine = in.readLine()) != null)
                json += inputLine;
            System.out.println(json.toString());
            in.close();
        } catch(Exception ex){
            System.out.println("Dweet Error");
            System.out.println(ex.toString());
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
