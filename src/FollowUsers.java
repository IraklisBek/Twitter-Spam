package twitterspam;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import twitter4j.IDs;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class  FollowUsers{
   
    @SuppressWarnings("SleepWhileInLoop")
    public static void main(String[] args) throws UnknownHostException, TwitterException, InterruptedException {
        
        DB dB_Trends = (new MongoClient("localhost", 27017)).getDB("Trends"); 
        DBCollection dBCollectionTrends = dB_Trends.getCollection("Trends");
        
        DB dB_Tweets = (new MongoClient("localhost", 27017)).getDB("Tweets");
        DBCollection dBCollectionTweets = dB_Tweets.getCollection("Tweets");
        
        DB dB_DataNeed = (new MongoClient("localhost", 27017)).getDB("DataNeed");
        DBCollection dBCollectionDataNeed = dB_DataNeed.getCollection("DataNeed"); 
        DBCursor dbCursorData = dBCollectionDataNeed.find();
        DBCursor dbCursorData2 = dBCollectionDataNeed.find();  
        DBCursor dbCursorData3 = dBCollectionDataNeed.find();
        DBCursor dbCursorData4 = dBCollectionDataNeed.find();
        DBCursor dbCursorData5 = dBCollectionDataNeed.find();
        
        DB usersClues = (new MongoClient("localhost", 27017)).getDB("UsersClues");
        DBCollection dBCollectionUsersClues = usersClues.getCollection("UsersClues");
        DBCursor dbCursorUsersClues = dBCollectionUsersClues.find();                        
        
        HashMap<Long, HashSet<String>> userTrends = new HashMap<>();//Key id, value the trends.
        HashMap<Long, Integer> userIDTrendsNumber = new HashMap<>();//Key id, value trends number referance.
        
        ArrayList<Long> userIDs = new ArrayList<>();//Ta μοναδικά userID.
        ArrayList<Integer> trendsSizes = new ArrayList<>();//Ο αριθμός των trends που αναφέρονται οι users. 
        
        ArrayList<String> dataTrendsDB = new ArrayList<>();     String dataTrend;   //ArrayList που μαζεύουμε τα Trends απο το db που έχουμε τα data που θέλουμε.
        ArrayList<Long> dataIDs = new ArrayList<>();            long id;            //ArrayList που μαζεύουμε τα IDs απο το db που έχουμε τα data που θέλουμε.      
        ArrayList<Integer> friendsOfId = new ArrayList<>();     int friend;         //Όμοια.
        ArrayList<Integer> followersOfId = new ArrayList<>();   int follower;       //Όμοια.
        ArrayList<Date> createdId = new ArrayList<>();          Date date;          //Όμοια.
        
        while(dbCursorData.hasNext()){
            id = (long) dbCursorData.next().get("userId");          //Παίρνουμε το ID από το dbDataNeed.
            dataIDs.add(id);                                        //Το βάζουμε στο ArrayList.
            
            dataTrend = (String) dbCursorData2.next().get("trend"); //Παίρνουμε το ID από το dbDataNeed.
            dataTrendsDB.add(dataTrend);                            //Το βάζουμε στο ArrayList. 
            
            friend = (int) dbCursorData3.next().get("friends");     //Όμοια.
            friendsOfId.add(friend);                            
            
            follower = (int) dbCursorData4.next().get("followers"); //Όμοια.
            followersOfId.add(follower);                           
            
            date = (Date) dbCursorData5.next().get("acount_duration"); //Όμοια.
            createdId.add(date);    
            
                      
                        //Οι θέσεις των ArrayLists dataTrendsDB, dataIDs θα είναι αντίστοιχες για userID 
                        //+και Trend στο οποίο αναφέρεται. 
                        //Δηλαδή θέση 0 του dataIDs θα είναι ένα userID
                        //+και θέση 0 του dataTrendsDB θα είναι το Trend στο οποίο αναφέρεται ο userID της θέσης 0 του dataIDs κ.ο.κ.
        }

        
        //Δημιουργείται το HashMap με κλειδί το id του user και τιμή τα trends στα οποία αναφέρεται.
        for(int j=0; j<dataTrendsDB.size(); j++){
            if(userTrends.containsKey(dataIDs.get(j))){
                HashSet<String> trends = userTrends.get(dataIDs.get(j));
                if(!trends.contains(dataTrendsDB.get(j))){
                       trends.add(dataTrendsDB.get(j)); 
                }                   
            }else{
                HashSet<String> trends = new HashSet<>();
                trends.add(dataTrendsDB.get(j));
                userTrends.put(dataIDs.get(j), trends);
                //Εδώ βάζουμε σε db τους Users και τους φίλους, ακόλουθους και δημιουργία των account τους. Xρήσιμο για το Α4.
                try{
                    float followersToFriends;
                    followersToFriends=(float) followersOfId.get(j)/friendsOfId.get(j);
                    Date dateNow = new Date();
                    BasicDBObject userActivity= new BasicDBObject("userId", dataIDs.get(j))
                                    .append("acount_duration", (dateNow.getTime() - createdId.get(j).getTime())/1000/60/60/24)//Η ηλικία του λογαριασμού σε μέρες.
                                    .append("followers", followersOfId.get(j))
                                    .append("friends", friendsOfId.get(j))
                                    .append("followersToFriends ", followersToFriends)                            
                                    ;
                    dBCollectionUsersClues.insert(userActivity);
                }catch(Exception r){
                    
                }
            }
        } 
        //Δημιουργείται το HashMap με κλειδί το id του user και τιμή τον αριθμό των trends στα οποία αναφέρεται.
        for(Long userID : userTrends.keySet()){
            HashSet<String> trends = userTrends.get(userID);
            userIDTrendsNumber.put(userID, trends.size());
        }
        //Δημιουργείται το ArrayList με τα μοναδικά userID και το ArrayList με τους μοναδικούς αριθμούς αναφορών σε trends.
        //για την δημιουργία των Quarters.
        for(Long userID : userIDTrendsNumber.keySet()){
            userIDs.add(userID);
            if(!trendsSizes.contains(userIDTrendsNumber.get(userID))){
                trendsSizes.add(userIDTrendsNumber.get(userID));
            }
        }
        Collections.sort(trendsSizes);                  
        List<Long> ids = new ArrayList<>(userIDTrendsNumber.keySet());
        Collections.sort(ids, new Comparator<Long>() {

            @Override
            public int compare(Long o1, Long o2) {
                return userIDTrendsNumber.get(o2).compareTo(userIDTrendsNumber.get(o1));
            }
        });
        //Υπολογισμός τεταρτιμόριων.
        int q1 =(trendsSizes.get(0) + trendsSizes.get(trendsSizes.size()/2)) /2 ;
        int q2=trendsSizes.get(trendsSizes.size()/2);
        int q3= (trendsSizes.get(trendsSizes.size()/2) + trendsSizes.get(trendsSizes.size()-1)) /2;
        System.out.println(q1 + " " + q2 + " " + q3);
        //Counters για υπολογισμό χρηστών ανα τεταρτιμόριο. Χρήσιμο για εύρος τυχαίων αριθμών.
        int counterQ1Users=0;
        int counterQ2Users=0;
        int counterQ3Users=0;
        int counterQ4Users=0;
        //Δίνουμε τημές σε μετρητές όπου μετράμε το πόσα ids αναφέρονται σε αριθμούς αναφορών ανάλογα με τα quorters που δημιουργήθηκαν.
        //Έχει γίνει sort του hasmap με βάσητους αριθμούς αναφορών το οποίο βοηθάει στην σωστή καταμέτρηση των Users βάση του quorter στο οποίο ανήκουν.
        for (Long iD : ids) {
            if(userIDTrendsNumber.get(iD)<q1){
                counterQ1Users++;
            }
            if(userIDTrendsNumber.get(iD)>=q1 && userIDTrendsNumber.get(iD)<q2){
                counterQ2Users++;
            }
            if(userIDTrendsNumber.get(iD)>=q2 && userIDTrendsNumber.get(iD)<q3){
                counterQ3Users++;
            }  
            if(userIDTrendsNumber.get(iD)>=q3){//edw hxame to q3 20 alla afou etrekse 7 meres katalavame oti prepei na einai 24 (grammi 141).
                                               //en telh omws kalitera giati kanoume elegxo se perisoterous users.
                counterQ4Users++;
            }             
        }

        //Lists όπου θα αποθηκευτούν οι τυχαίοι users βάση των quorters που βρήσκονται.
        ArrayList<Long> usersQ1= new ArrayList<>();
        ArrayList<Long> usersQ2= new ArrayList<>();
        ArrayList<Long> usersQ3= new ArrayList<>();
        ArrayList<Long> usersQ4= new ArrayList<>();
        usersQAdd(userIDs, usersQ1, counterQ1Users, 0);
        usersQAdd(userIDs, usersQ2, counterQ2Users, counterQ1Users);
        usersQAdd(userIDs, usersQ3, counterQ3Users, counterQ1Users + counterQ2Users);
        usersQAdd(userIDs, usersQ4, counterQ4Users, counterQ1Users + counterQ2Users + counterQ3Users);       
            
        ArrayList<Long> selectedUsersIDs = new ArrayList<>();
        for (Long user : usersQ1) {
            selectedUsersIDs.add(user);
        }
        for (Long user : usersQ2) {
            selectedUsersIDs.add(user);
        }
        for (Long user : usersQ3) {
            selectedUsersIDs.add(user);
        }
        for (Long user : usersQ4) {
            selectedUsersIDs.add(user);
        }        

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
        .setOAuthConsumerKey("WPDndMEq4X6ltOoMHHaGarpJl")
        .setOAuthConsumerSecret("5rKcCtr0nTbrubN4EsTtND3NPgINDPbTHg9j44cEEwueEWnztm")
        .setOAuthAccessToken("2871096179-3fJ8Xm0jRAHuFuiPVPCxPWiMs2buoB9ZEubGunr")
        .setOAuthAccessTokenSecret("0R8NqWV6trsbDjsN8CHaMFkGfHsj0053ZO3a9esJ2sz6q");                
                        
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance(); 
        
        DB dbSelectedUsersActivity = (new MongoClient("localhost", 27017)).getDB("SelectedUsersActivity"); 
        DBCollection dbCollectionSelectedUsersActivity = dbSelectedUsersActivity.getCollection("SelectedUsersActivity"); 
        DBCursor dbCursorSelectedUsersActivity = dbCollectionSelectedUsersActivity.find();
        
        List<Status> statuses = new ArrayList<>();
        ArrayList<Long> tweetIDs = new ArrayList<>();
        ArrayList<Long> illegalRemoves = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        while((System.currentTimeMillis()-startTime)<604800000){
            insert(illegalRemoves, tweetIDs, statuses, selectedUsersIDs, dbCursorSelectedUsersActivity, dbCollectionSelectedUsersActivity, twitter);  
            Thread.sleep(900000);
        }                
    }  
    public static void usersQAdd(ArrayList<Long> userIDs, ArrayList<Long> usersQ, int counter, int previousCounters){
        Random rand = new Random();
        ArrayList<Integer> excl = new ArrayList<>();
        int r=0;        
        if(counter>9){
            while(r<10){
                int randomNum = previousCounters + rand.nextInt(counter);
                if(!excl.contains(randomNum)){                    
                    usersQ.add(userIDs.get(randomNum));
                    excl.add(randomNum);
                    r++;
                }
            }
        }else{
            for(int i=previousCounters; i<previousCounters+counter; i++){
                usersQ.add(userIDs.get(i));
            }
        }
    }
    
    public static void insert(ArrayList<Long> illegalRemoves, ArrayList<Long> tweetids, List<Status> statuses, ArrayList<Long> users, DBCursor dbCursor, DBCollection collection, Twitter twitter) throws TwitterException{
        boolean flag;
        for(int i=0; i<users.size(); i++){
            try{
                statuses = twitter.getUserTimeline(users.get(i));                
                for(Status status:statuses){                    
                    if(!(tweetids.contains(status.getId()))){ 
                        tweetids.add(status.getId());
                        BasicDBObject userActivity= new BasicDBObject("tweetid", status.getId())
                                    .append("userID", status.getUser().getId())
                                    .append("text", status.getText()  )
                                    .append("followers", status.getUser().getFollowersCount())
                                    .append("friends", status.getUser().getFriendsCount())
                                    .append("acount_duration", status.getUser().getCreatedAt())
                                    .append("retweets", status.getRetweetCount())
                                    .append("repliesOfUser", status.getInReplyToUserId())
                                    .append("mentions", status.getUserMentionEntities().length)
                                    .append("URL", status.getURLEntities().length)
                                    .append("hashtag", status.getHashtagEntities().length)
                                    .append("source", Jsoup.parse(status.getSource()).text())
                                    ;  
                        System.out.println(userActivity);
                        //System.out.println("SOURCE: " + Jsoup.parse(status.getSource()).text());
                        try{
                            collection.insert(userActivity);
                        }catch(Exception ex){
                        }
                        System.out.println(userActivity);
                    }
                }
            }catch(Exception np){
                System.out.println("\n****************\n****************\n****************"
                        + "\n****************\n****************"
                        + "\n****************\n****************\n****************");
            }
        }         
    }
}
