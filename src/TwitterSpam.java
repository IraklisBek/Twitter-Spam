package twitterspam;

import com.mongodb.*;
import com.mongodb.util.JSONParseException;
import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.*;

public class TwitterSpam implements Serializable {

    

    @SuppressWarnings("SleepWhileInLoop")
    public static void main(String[] args) throws JSONParseException,  UnknownHostException, TwitterException, InterruptedException, ConnectException, MongoTimeoutException,   IOException {
        long startTime = System.currentTimeMillis();
        
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                    /**
                     * Authentication keys.
                     */
        .setOAuthConsumerKey("WPDndMEq4X6ltOoMHHaGarpJl")
        .setOAuthConsumerSecret("5rKcCtr0nTbrubN4EsTtND3NPgINDPbTHg9j44cEEwueEWnztm")
        .setOAuthAccessToken("2871096179-3fJ8Xm0jRAHuFuiPVPCxPWiMs2buoB9ZEubGunr")
        .setOAuthAccessTokenSecret("0R8NqWV6trsbDjsN8CHaMFkGfHsj0053ZO3a9esJ2sz6q");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
                    /**
                     * Data Base calls.
                     */
        DB dB_Trends = (new MongoClient("localhost", 27017)).getDB("Trends"); 
        DB dB_Tweets = (new MongoClient("localhost", 27017)).getDB("Tweets");
        DB dB_DataNeed = (new MongoClient("localhost", 27017)).getDB("DataNeed");
        DBCollection dBCollectionTrends = dB_Trends.getCollection("Trends");
        DBCollection dBCollectionTweets = dB_Tweets.getCollection("Tweets");
        DBCollection dBCollectionDataNeed = dB_DataNeed.getCollection("DataNeed");       
                    /**
                     * ArrayList curTrends τα trends που θα μπουν. 1 εμφάνιση για κάθε trend .
                     * ArrayList activeIssues για αποθήκευση των ενεργών θεμάτων .
                     */
        ArrayList<Trend> curTrends = new ArrayList<>();
        ArrayList<String> activeIssues = new ArrayList<>();
                    /**
                     * Αρχικές τημές σε time start και end.
                     */
        long time_start;
        long time_end;
        long new_time_end;
        long time;
        int elementCounter=0;
        long keepEndTime[] = new long[1000000];
        Query query;
        while((System.currentTimeMillis()-startTime)<259200000){
            try{    
                Trends trends=twitter.getPlaceTrends(1); // Για να παίρνει τα παγκόσμια popoular trends.
                Trend[] allFirst;            // Δημιουργία πίνακα όπου θα αποθηκεύονται κάθε φορά τα 10 trends 
                allFirst=trends.getTrends(); //+αφού περάσουν 5 λεπτά .   
                for(int i=0; i<10; i++){
                    boolean flag=true;//Συσχέτιση με γραμμή 85.
                    for (int j=0; j<curTrends.size();j++) {
                        if (curTrends.get(j).getName().equals(allFirst[i].getName())){  //Σύκρινε όλα τα στοιχεία του curTrends με ένα συγκεκριμένο <<καινούργιο>> trend .                                                                                                     
                            flag=false;// Το flag γίνεται false έτσι ώστε να μην γίνει το add το καινούργιο trend στο curTrends.                                  
                            String f = Integer.toString(j);
                            new_time_end=System.currentTimeMillis();
                            BasicDBObject newstuff = new BasicDBObject();//Object για τα καινούργια δεδομένα.
                            newstuff.append("$set", new BasicDBObject().append("end_date", new_time_end));
                            BasicDBObject queryOf = new BasicDBObject(f, curTrends.get(j).getName());//Βρίσκουμε το trend
                            dBCollectionTrends.update(queryOf, newstuff);//Δίνουμε στο trend την καινούργια τιμή.
                            keepEndTime[j] = System.currentTimeMillis(); //Κρατάει του χρόνους λήξης σε πίνακα.
                        }
                    }
                    if(flag){                                   
                        curTrends.add(allFirst[i]);
                        activeIssues.add(allFirst[i].getName());
                    }
                }
                                /**
                                 * Όσο ο counter είναι μικρότερος απο το size του ArrayList 
                                 * θα πάρει ακριβώς όλα τα στοιχεία του curTrendsώστε να τα τοποθετήσει στη βάση δεδομένων. 
                                 */ 
                while(elementCounter<curTrends.size()){   // Eπειδή στο ArrayList τα στοιχεία θα είναι μοναδικά θα πρέπει να έχουν και 1 μοναδικό key.               
                    time_start=System.currentTimeMillis();
                    time_end=System.currentTimeMillis();
                    keepEndTime[elementCounter] = time_end;//Κρατάει και εδώ του χρόνουσ λήξης 
                    String trendKey = Integer.toString(elementCounter); //+Εδώ δίνουμε το key. 
                    BasicDBObject trend = new BasicDBObject(trendKey, curTrends.get(elementCounter).getName())//Εδώ αποθηκεύουμε στο DBObject το όνομα του trend.
                                                       .append("start_date", time_start)
                                                       .append("end_date", time_end);
                    System.out.println(trend);
                    dBCollectionTrends.insert(trend);  
                    elementCounter++; //Συσχέτιση με γραμμή 96-98.
                }
                for(int i=0;i<curTrends.size();i++){
                    time=System.currentTimeMillis();
                        if(keepEndTime[i] + 7200000 < time){
                            activeIssues.remove(curTrends.get(i).getName());
                        }
                }       
                //Συλλογή των Tweets.
                int c=0;
                for (String activeIssue : activeIssues) {
                    c++;
                    query = new Query(activeIssue);
                    QueryResult result;           
                    result = twitter.search(query);
                    List<Status> tweets = result.getTweets();
                    for (Status tweet : tweets) {
                        if (result.getQuery().equals(activeIssue)) {     
                            String aaTweet = tweet.toString();
                            BasicDBObject aTweet = new BasicDBObject(activeIssue, aaTweet);
                            BasicDBObject fTweet= new BasicDBObject("userId", tweet.getUser().getId())
                                            .append("text", tweet.getText()  )
                                            .append("followers", tweet.getUser().getFollowersCount())
                                            .append("friends", tweet.getUser().getFriendsCount())
                                            .append("acount_duration", tweet.getUser().getCreatedAt())
                                            .append("retweets", tweet.getRetweetCount())
                                             //.append("retweeted", tweet.getRetweetedStatus())
                                            .append("repliesOfUser", tweet.getInReplyToUserId())
                                            .append("URL", tweet.getURLEntities().length)
                                            .append("hashtag", tweet.getHashtagEntities().length)
                                            .append("trend", activeIssue)
                                            ;
                            System.out.println(fTweet);
                            try{
                                dBCollectionTweets.insert(aTweet);
                                dBCollectionDataNeed.insert(fTweet);
                            }catch (Exception ex){
                            } 
                        }
                    }
                }
                Thread.sleep(300000); //sleep 5 minutes
            }catch(Exception e){
                
            }
        }     
    }
    
}
