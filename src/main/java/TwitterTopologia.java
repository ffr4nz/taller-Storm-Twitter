import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.stats.RollingWindow;
import backtype.storm.topology.TopologyBuilder;
import bolt.FileWriterBolt;
import bolt.HashtagExtractionBolt;
import bolt.LanguageDetectionBolt;
import bolt.RollingCountBolt;
import spout.TwitterSpout;
import twitter4j.FilterQuery;


public class TwitterTopologia {

//    private static String consumerKey = "FILL IN HERE";
//    private static String consumerSecret = "FILL IN HERE";
//    private static String accessToken = "FILL IN HERE";
//    private static String accessTokenSecret = "FILL IN HERE";
    private static String consumerKey = "rR7yrSFfb05Dj7sfF6J57dz0G";
    private static String consumerSecret = "8IYdN3VuzqtBQx9VF67MAUzlFLboOlteILYXocuH3Z3NRlxPZB";
    private static String accessToken = "176882080-xl7GJfkdGPO74iIHGVut9Hk8Z6ScxlLZy7pdHOBQ";
    private static String accessTokenSecret = "7uJSrDS2OIDTBWnGZwz0p4J6RAdT6FmbhTJztjFAtL35k";



    public static void main(String[] args) throws Exception {
        /**************** SETUP ****************/
        String remoteClusterTopologyName = null;
        if (args!=null) {
            if (args.length==1) {
                remoteClusterTopologyName = args[0];
            }
            // If credentials are provided as commandline arguments
            else if (args.length==4) {
                consumerKey =args[0];
                consumerSecret =args[1];
                accessToken =args[2];
                accessTokenSecret =args[3];
            }

        }
        /****************       ****************/

        TopologyBuilder builder = new TopologyBuilder();

        FilterQuery tweetFilterQuery = new FilterQuery();
        // TODO: Define your own twitter query
         tweetFilterQuery.track(new String[]{"madrid"});
        // See https://github.com/kantega/storm-twitter-workshop/wiki/Basic-Twitter-stream-reading-using-Twitter4j


        TwitterSpout spout = new TwitterSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, tweetFilterQuery);
        //TODO: Set the twitter spout as spout on this topology. Hint: Use the builder object.

        FileWriterBolt fileWriterBolt = new FileWriterBolt("contador.txt");
        //TODO: Route messages from the spout to the file writer bolt. Hint: Again, use the builder object.

        LanguageDetectionBolt lenguaje= new LanguageDetectionBolt();
        HashtagExtractionBolt hashtag = new HashtagExtractionBolt();
        RollingCountBolt contador = new RollingCountBolt(9, 3);

        builder.setSpout("spoutLeerTwitter",spout,1);
        builder.setBolt("lenguaje",lenguaje,1).shuffleGrouping("spoutLeerTwitter");
        builder.setBolt("hashtag",hashtag,1).shuffleGrouping("lenguaje");
        builder.setBolt("cont",contador,1).shuffleGrouping("hashtag");
        builder.setBolt("escribirFichero",fileWriterBolt,1).shuffleGrouping("cont");


        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("twitter-fun", conf, builder.createTopology());

            Thread.sleep(460000);

            cluster.shutdown();
        }
    }
}
