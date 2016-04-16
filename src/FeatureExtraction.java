package twitterspam;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.validator.UrlValidator;
import org.jsoup.Jsoup;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;


public class FeatureExtraction {

    private static ArrayList<String> URLS;
    private static int URLcounter;
    private static ArrayList<String> domains;
    private static int domainCounter;
    
    public static void main(String[] args) throws UnknownHostException, InterruptedException, IOException, URISyntaxException {
        URLcounter=0;
        URLS=new ArrayList<String>();
        domains=new ArrayList<String>();
        domainCounter=0;
        String testURL=expandShortURL("http://t.co/5zSDBsc1VL");
        testURL=getDomainName(testURL);
        System.out.println("TEST DOMAIN:  " + testURL);
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
        QuadrantInformation qInfo=new QuadrantInformation();        
        
        long[] selectedUsersIDs = getUsersIDs(dbCollectionSelectedUsersActivity);
        
        System.out.println("********    FEATURE EXTRACTION     ********");
        FeatureExtraction(selectedUsersIDs,dbCursorSelectedUsersActivity,dbCollectionSelectedUsersActivity,qInfo,twitter);
        System.out.println("");
                       
    }              
    
    private static int Minimum(int v1, int v2, int v3){
        if(v1<=v2 && v1<=v3)            
            return v1;
        if(v2<=v1 && v2<=v3)
            return v2;
        return v3;
    }
    
    public static int LevenshteinDistance(String s, String t)
    {
        if (s.equals(t)) return 0;
        if (s.length()==0) return t.length();
        if (t.length()==0) return s.length();
        int[] v0 = new int[t.length()+1];
        int[] v1 = new int[t.length()+1];
        for (int i=0;i<v0.length;i++)
            v0[i]=i;
        for (int i=0;i<s.length();i++)
        {
            v1[0]=i+1;
            for(int j=0;j<t.length();j++)
            {
                int cost=(s.charAt(i)==t.charAt(j)) ? 0 : 1;
                v1[j+1]=Minimum(v1[j]+1,v0[j+1]+1,v0[j]+cost);
            }
            for (int j=0;j<v0.length;j++)
                v0[j]=v1[j];
        }
        return v1[t.length()];
    }
    
    private static String removeURLS(String s){
        /* afairesh URL*/
        String s1=s;
        String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
        Pattern p = Pattern.compile(urlPattern,Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(s1);
        int i = 0;
        while (m.find()) {
            try{
                s1 = s1.replaceAll(m.group(i),"").trim();
                
            }catch (java.util.regex.PatternSyntaxException e){       
                /* 
                Sfalma sto pattern antimetwpizetai me to na afaireitai h esfalmenh grammatoseira mesa apo 
                thn arxikh kai ksanakaleitai h idia methodos
                */
                return removeURLS(s.replace(e.getPattern(), ""));
            }
            i++;
        }
        return s1;
    }
    
    private static String removeMentions(String s1){
        /* afairesh Mentions, mpakalikos tropos giati an ena mention exei underscore (p.x. @win_aero) tote den douleuei h kanonikh ekfrash */
        /*  .replaceAll("@\\p{L}+", "");    */
        
        if(s1.contains("@")){                    
            StringBuilder str=new StringBuilder();
            for(int j=0;j<s1.length();j++){
                if(s1.charAt(j)=='@'){
                    //sunexise mexri na breis to epomeno keno
                    do{
                        j++;
                    }while(s1.charAt(j)!=' ' && j!=s1.length()-1);
                }
                else{
                    str.append(s1.charAt(j));
                }
            }
            s1=str.toString();
        }        
        return s1;
    }
    
    public static void FeatureExtraction(long[] usersQ, DBCursor dbCursor, DBCollection collection,QuadrantInformation qInfo, Twitter twitter) throws InterruptedException, IOException, URISyntaxException {
        /*
        ftiaxnoume ArrayList apo ArrayList (meeeh)
        kathe (anwtero epipedo) arrayList antistoixei ston kathe xrhsth tou trexontos tetarthmoriou
        k kathe stoixeio (katwterou epipedou) arrayList antistoixei sto keimeno twn tweets tou xrhsth
        xwris mentions (@...) kai URLS.
        */
        System.out.println("Beginning Feature Extraction");
        ArrayList<ArrayList<String>> userTweets = new ArrayList<ArrayList<String>>(usersQ.length);
        ArrayList<ArrayList<String>> userTweetIDs = new ArrayList<ArrayList<String>>(usersQ.length);
                       
        for(int i=0;i<usersQ.length;i++){
            
            /***************************/
            ArrayList<String> userURLS=new ArrayList<String>();
            int userURLcounter=0;
            ArrayList<String> userDomains=new ArrayList<String>();
            int userDomainCounter=0;
            /***************************/
            System.out.println("Current user: " + (i+1) + "/" + usersQ.length);
            //System.out.println(usersQ[i]);
            
            ArrayList<String> user_i=new ArrayList<String>();            
            ArrayList<String> tweet_i=new ArrayList<String>();
            ArrayList<String> source=new ArrayList<String>();
            
            userTweets.add(user_i);  
            userTweetIDs.add(tweet_i);
            /*
            bres mou oles tis kataxwrhseis sth bash tou trexontos xrhsth
            */
            DBCursor currentUser=collection.find(new BasicDBObject("userID",usersQ[i]));
            /*
            bres mou ola ta tweetids pou anhkoun ston trexonta xrhsth
            */
            //DBCursor currentTweetID=collection.find();
            //BasicDBObject obj=new BasicDBObject();
            //System.out.println("Reading tweets of current user");
            while(currentUser.hasNext()){
                Object tweets = currentUser.next().get("text");
                String text=removeMentions(removeURLS(tweets.toString()));                
                userTweets.get(i).add(text);
                /**/
                String url=removeURLS(tweets.toString());
                
                try{
                    url=tweets.toString().replaceAll(url, "");
                }catch (java.util.regex.PatternSyntaxException e){
                    continue;
                }
                if(url.isEmpty() || !(new UrlValidator().isValid(url)) )   continue;                                
                String expanded=expandShortURL(url);
                //System.out.println("Expanded: " + expanded);
                String domainName=getDomainName(expanded);
                //System.out.println("Domain Name: " + domainName);
                if(!expanded.isEmpty()){
                    if(!URLS.contains(expanded)){
                        URLS.add(expanded);                        
                    }
                    if(!userURLS.contains(expanded))
                        userURLS.add(expanded);
                    URLcounter++;
                    userURLcounter++;
                    
                    if(!domains.contains(domainName))
                        domains.add(domainName);                    
                    if(!userDomains.contains(domainName))
                        userDomains.add(domainName);
                    userDomainCounter++;
                    domainCounter++;
                }
            }
            currentUser=collection.find(new BasicDBObject("userID",usersQ[i]));
            //System.out.println("Reading tweet IDs of current user");
            while(currentUser.hasNext()){
                Object tweets = currentUser.next().get("tweetid");
                String text=tweets.toString();
                userTweetIDs.get(i).add(text);                      
            }
            currentUser=collection.find(new BasicDBObject("userID",usersQ[i]));
            //System.out.println("Reading sources of user tweets");
            while(currentUser.hasNext()){
                Object tweets = currentUser.next().get("source");
                if(tweets==null)    continue;
                String text=tweets.toString();
                source.add(text);                      
            }
            
            
        
        /*
        brikame ola ta tweets twn xrhstwn tou trexontos tetarthmoriou
        opws epishs kai ta IDS twn tweets
        kai twra prepei na upologisoume thn zhtoumenh apostash (ana duo)
        kai tha ta kataxwrhsoume se ena map
        */
       
            //Map<Map<String,String>,Integer> mapping=new HashMap<Map<String,String>,Integer>();  //mono gia ton ena xrhsth
            /*
            upologizoume thn Levenshtein apostash
            apo i ws length-1
            apo i+1 ws length
            */
            qInfo.addUser(Long.toString(usersQ[i]));
            ArrayList similarTweets=new ArrayList<String>();
            System.out.println("Overall Number of tweets for current user: " + userTweets.get(i).size());
            for(int k=0;k<userTweets.get(i).size()-1;k++){
                if(userTweets.get(i).isEmpty())    continue;
                //if(userTweetIDs.get(i).get(k).length()==0)   continue;
                qInfo.addTweetSource(Jsoup.parse(source.get(k)).text());
                
                for(int j=k+1;j<userTweets.get(i).size();j++){
                    if(similarTweets.contains(userTweets.get(i).get(j)) && similarTweets.contains(userTweets.get(i).get(k))) continue;
                    //if(userTweetIDs.get(i).get(j).length()==0)   continue;
                    double levDist=(double)LevenshteinDistance(                     //Kanonikopoiimenh: apostash/athroisma strings
                                    userTweets.get(i).get(k),
                                    userTweets.get(i).get(j)
                            );
                    //System.out.println("Levehstein Distance: " + levDist);
                    double distance=levDist/((double)(userTweets.get(i).get(k).length() + userTweets.get(i).get(j).length()));
                    //System.out.println("Distance: " + distance);
                    if(distance<=0.1){
                        qInfo.addMapValue(
                                userTweetIDs.get(i).get(k),
                                userTweetIDs.get(i).get(j)
                        );
                        //System.out.println("");
                        //System.out.println("The tweets: (index="+k+"): ");
                        //System.out.println(userTweets.get(i).get(k));
                        //System.out.println(" and: (index="+j+"): ");
                        //System.out.println(userTweets.get(i).get(j));
                        //System.out.println("are similar.");
                        //System.out.println("");
                        similarTweets.add(userTweets.get(i).get(k));
                        similarTweets.add(userTweets.get(i).get(j));
                        //System.out.println("TweetIDs:\t"+userTweetIDs.get(i).get(k)+"\t"+userTweetIDs.get(i).get(j));
                    }
                }
            }
            qInfo.addTweetSource(Jsoup.parse(source.get(userTweets.get(i).size()-1)).text());
            DB useData = (new MongoClient("localhost", 27017)).getDB("UseData");
            DBCollection dBCollectionUseData = useData.getCollection("UseData");    
            int suspiciusRateSimilarTeetsAndPostTools;
            int suspiciusRateSimilar=0;
            int suspiciusRatePosts = qInfo.getPostToolNumberSusRate();
            int similarTweets2 = qInfo.getSimilarTweets(Long.toString(usersQ[i]));
            if(similarTweets2>40){
                suspiciusRateSimilar=5;
            }
            suspiciusRateSimilarTeetsAndPostTools=suspiciusRateSimilar + suspiciusRatePosts;
            BasicDBObject useDat= new BasicDBObject("UserID", usersQ[i])
                    .append("SuspiciusRateSimilarTeetsAndPostTools", suspiciusRateSimilarTeetsAndPostTools)
                    ;
            dBCollectionUseData.insert(useDat);
            System.out.println("Similar Tweets for i: " + usersQ[i]);
            System.out.println(similarTweets2);
            System.out.println(useDat);
            qInfo.printTweetSources();
            System.out.println("");
            System.out.println("monadika URLs / sunolika URLs : "+((float)userURLS.size()/(float)userURLcounter));
            System.out.println("monadika domains / sunolika domains : " + ((float)userDomains.size()/(float)userDomainCounter));
            System.out.println("**********************************************");
        }
        System.out.println("FOUND URLS: ");
        for(int i=0;i<URLS.size();i++)
            System.out.println("URL("+(i+1)+"): "+URLS.get(i));
        System.out.println("monadika URLs / sunolika URLs : "+((float)URLS.size()/(float)URLcounter));
        System.out.println("monadika domains / sunolika domains : " + ((float)domains.size()/(float)domainCounter));
        System.out.println("monadika urls: " + URLS.size());
        System.out.println("sunolika: " + URLcounter);
        System.out.println("monadika domains: " + domains.size());
        System.out.println("Sunolika domains: " + domainCounter);
    }

    private static long[] getUsersIDs(DBCollection dbCursorSelectedUsersActivity) {
        DBCollection allUsers=dbCursorSelectedUsersActivity;
        DBCursor findIDs=allUsers.find();
        ArrayList<Long> allIDs=new ArrayList<Long>();
        while(findIDs.hasNext()){
            Object id=findIDs.next().get("userID");
            long temp=Long.parseLong(id.toString());
            if(allIDs.isEmpty() || !allIDs.contains(temp)){
                allIDs.add(temp);
            }
        }
        long[] userID=new long[allIDs.size()];
        for(int i=0;i<allIDs.size();i++)
            userID[i]=(long)allIDs.get(i);
        return userID;
    }
    
    private static String expandShortURL(String address) throws IOException {
        if(address.equals("0")||address==null||address.isEmpty())
            return null;
        URL url = new URL(address);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY); //using proxy may increase latency
        connection.setInstanceFollowRedirects(false);
        connection.connect();
        String expandedURL = connection.getHeaderField("Location");
        connection.getInputStream().close();
        return expandedURL;
    }
    private static String getDomainName(String url) throws URISyntaxException {
        if(url==null||url.isEmpty())
            return null;
        URI uri = new URI(url);
        String domain = uri.getHost();
        return domain.startsWith("www.") ? domain.substring(4) : domain;
    }
}