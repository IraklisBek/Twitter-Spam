package twitterspam;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class UseData {
    public static void main(String[] args) throws UnknownHostException {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
        .setOAuthConsumerKey("WPDndMEq4X6ltOoMHHaGarpJl")
        .setOAuthConsumerSecret("5rKcCtr0nTbrubN4EsTtND3NPgINDPbTHg9j44cEEwueEWnztm")
        .setOAuthAccessToken("2871096179-3fJ8Xm0jRAHuFuiPVPCxPWiMs2buoB9ZEubGunr")
        .setOAuthAccessTokenSecret("0R8NqWV6trsbDjsN8CHaMFkGfHsj0053ZO3a9esJ2sz6q");                
                        
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance(); 
        DB useData = (new MongoClient("localhost", 27017)).getDB("UseData");
        DBCollection dBCollectionUseData = useData.getCollection("UseData");
        DBCursor dbCursorUseData = dBCollectionUseData.find();  
        DBCursor dbCursorUseData1 = dBCollectionUseData.find();
        
        DB dbSelectedUsersActivity = (new MongoClient("localhost", 27017)).getDB("SelectedUsersActivity"); 
        DBCollection dbCollectionSelectedUsersActivity = dbSelectedUsersActivity.getCollection("SelectedUsersActivity"); 
        DBCursor dbCursorSelectedUsersActivity = dbCollectionSelectedUsersActivity.find(); 
        DBCursor dbCursorSelectedUsersActivity1 = dbCollectionSelectedUsersActivity.find();
        DBCursor dbCursorSelectedUsersActivity2 = dbCollectionSelectedUsersActivity.find();
        DBCursor dbCursorSelectedUsersActivity3 = dbCollectionSelectedUsersActivity.find();
        DBCursor dbCursorSelectedUsersActivity4 = dbCollectionSelectedUsersActivity.find();
        DBCursor dbCursorSelectedUsersActivity5 = dbCollectionSelectedUsersActivity.find();
        DBCursor dbCursorSelectedUsersActivity6 = dbCollectionSelectedUsersActivity.find();
        DBCursor dbCursorSelectedUsersActivity7 = dbCollectionSelectedUsersActivity.find();
        int friends;
        int followers;
        Date duration;
        long daysDuration;
        long id;
        int mentions;
        int urls;
        int hashtags;
        int retweetsCount;
        int tweetsSum=0;
        //Φτιάχνουμε HashMaps όπου για κλειδί έχει το id, και value πληροφορίa που θέλουμε για το κάθε id με βάση τα χαρακτηριστικά των στατιστικών ερευνών.
        //Στο σύνολο τους έχουμε όλες τις πληροφορίες που θέλουμε.
        HashMap<Long, Double> hashtagsSum = new HashMap<>();
        HashMap<Long, Double> friendsPerFollowers = new HashMap<>();
        HashMap<Long, Long> accountDuration = new HashMap<>();
        HashMap<Long, Integer> retweets = new HashMap<>();
        HashMap<Long, Integer> postTools = new HashMap<>();
        HashMap<Long, Integer> finalSuspicusRate = new HashMap<>();
        int suspiciusSum=0;
        Date dateNow = new Date();
        
        while(dbCursorUseData.hasNext()){
            postTools.put((long)dbCursorUseData.next().get("UserID"), (int) dbCursorUseData1.next().get("SuspiciusRateSimilarTeetsAndPostTools"));
        }
        while(dbCursorSelectedUsersActivity.hasNext()){
            id = (long) dbCursorSelectedUsersActivity.next().get("userID");
            
            
            friends = (int) dbCursorSelectedUsersActivity1.next().get("friends");
            followers = (int) dbCursorSelectedUsersActivity2.next().get("followers");
            if(!friendsPerFollowers.containsKey(id)){
                friendsPerFollowers.put(id, (double) followers/friends);//<2
            }

            duration = (Date) dbCursorSelectedUsersActivity3.next().get("acount_duration");
            daysDuration = (dateNow.getTime() - duration.getTime())/1000/60/60/24;
            if(!accountDuration.containsKey(id)){
                accountDuration.put(id, daysDuration);//<180
            }
            
            mentions = (int) dbCursorSelectedUsersActivity4.next().get("mentions");
            
            urls = (int) dbCursorSelectedUsersActivity5.next().get("URL");
            
            hashtags = (int) dbCursorSelectedUsersActivity6.next().get("hashtag");
            if(hashtagsSum.containsKey(id)){
                tweetsSum++;
                hashtagsSum.put(id,(double) tweetsSum/(hashtags + hashtagsSum.get(id)));
                
            }else{
                tweetsSum++;
                hashtagsSum.put(id, (double) tweetsSum/hashtags);//hashtags/tweetsSum<0.05
                
            }            
            
            retweetsCount = (int) dbCursorSelectedUsersActivity7.next().get("retweets");
            if(retweets.containsKey(id)){
                retweets.put(id, retweetsCount + retweets.get(id));
            }else{
                retweets.put(id, retweetsCount);
            }                 
        }
        //HashMap με κλειδί το id και value το suspicious rate.
        for(Long iD : postTools.keySet()){
            suspiciusSum = postTools.get(iD);
            if(hashtagsSum.get(iD)<0.05){
                suspiciusSum = suspiciusSum + 1;
            }
            if(friendsPerFollowers.get(iD)<2){
                suspiciusSum = suspiciusSum + 1;
            }
            if(accountDuration.get(iD)<180){
                suspiciusSum = suspiciusSum + 1;
            }
            if(retweets.get(iD)<1000){
                suspiciusSum = suspiciusSum + 1;
            }
            
            finalSuspicusRate.put(iD, suspiciusSum);
            suspiciusSum=0;
        }
        for(Long aID : finalSuspicusRate.keySet()){
            if(finalSuspicusRate.get(aID)>10){
                System.out.println(aID + "  Has big chances of being a spammer");
                continue;
            }            
            if(finalSuspicusRate.get(aID)>8){
                System.out.println(aID + "  May be a spammer");
                continue;
            }
            if(finalSuspicusRate.get(aID)>4){
                System.out.println(aID + "  Has small chances of being a spammer");              
            }
        }
        for(Long aID : finalSuspicusRate.keySet()){
            System.out.println("ID: " + aID + "  Suspicius Rate: " + finalSuspicusRate.get(aID));
        }
    }   
}
