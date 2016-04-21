import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import bolt.FileWriterBolt;
import spout.TwitterSpout;
import twitter4j.FilterQuery;


public class TwitterTopologia {

    private static String consumerKey = "FILL IN HERE";
    private static String consumerSecret = "FILL IN HERE";
    private static String accessToken = "FILL IN HERE";
    private static String accessTokenSecret = "FILL IN HERE";


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

        FileWriterBolt fileWriterBolt = new FileWriterBolt("MyTweets.txt");
        //TODO: Route messages from the spout to the file writer bolt. Hint: Again, use the builder object.

        builder.setSpout("spoutLeerTwitter",spout,1);
        builder.setBolt("escribirFichero",fileWriterBolt,1).shuffleGrouping("spoutLeerTwitter");

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
