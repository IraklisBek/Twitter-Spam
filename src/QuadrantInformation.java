package twitterspam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;

public class QuadrantInformation {    
    /*
    epeidh ta keys einai monadika gia kathe user, den xreiazetai na xrhsimopoiisoume diaforetiko map,
    tha exoume ena monadiko map, me apothikeumena ta keys, gia olous tous xrhstes tou trexontos tetarthmoriou
    */
    private ArrayList<ArrayList<String>> keys;  //to prwto array list einai gia tous users
    private Map<String,String> map;
    private int users;  //xrhstes pou exoume kanei map
    private ArrayList<String> userID;
    private ArrayList<ArrayList<String>> tweetSource;
    private ArrayList<ArrayList<Integer>> sourceCounter;
    
    QuadrantInformation(){        
        keys=new ArrayList<ArrayList<String>>();        
        map=new HashMap<String,String>();
        userID=new ArrayList<String>();
        users=-1;   //tha paizei indexing rolo
        tweetSource=new ArrayList<ArrayList<String>>();
        sourceCounter=new ArrayList<ArrayList<Integer>>();
    }
    
    public void addUser(String userID){
        ArrayList<String> newEntry=new ArrayList<String>();
        ArrayList<String> newSourceEntry=new ArrayList<String>();
        ArrayList<Integer> newCounterEntry=new ArrayList<Integer>();
        keys.add(newEntry);
        tweetSource.add(newSourceEntry);
        sourceCounter.add(newCounterEntry);
        users++;
        this.userID.add(userID);
    }
    
    public int getNumberOfUsers(){
        return users;
    }
    
    public void addMapValue(String s1,String s2){
        if(users==-1){
            System.out.println("No user added");
            System.exit(1);
        }            
        map.put(s1, s2);
        keys.get(users).add(s1);
    }
    
    public void addMapValue(int user,String s1,String s2){
        if(user>users){
            System.out.println("No such user ");
            System.exit(1);
        }            
        map.put(s1, s2);
        keys.get(user).add(s1);
    }
    
    public void addMapValue(String userID,String s1,String s2){
        if(this.userID.indexOf(userID)==-1){
            System.out.println("No such user ");
            System.exit(1);
        }            
        map.put(s1, s2);
        keys.get(this.userID.indexOf(userID)).add(s1);
    }
    
    public int getSimilarTweets(String userID){
        int index=this.userID.indexOf(userID);
        if(index==-1)
            return -1;
        return keys.get(index).size();
    }
    
    public void addTweetSource(String source){
        if(tweetSource.get(users).isEmpty() || !tweetSource.get(users).contains(source)){            
            sourceCounter.get(users).add(1);
            tweetSource.get(users).add(source);
        }
        else{            
            int index=tweetSource.get(users).indexOf(source);
            sourceCounter.get(users).set(
                    index, 
                    sourceCounter.get(users).get(index)+1);                                
        }        
    }
    
    public void printTweetSources(){
        if(tweetSource.isEmpty())
            System.out.println("No tweet sources found");
        else{
            System.out.println("");
            System.out.println("Sources Found: " + tweetSource.get(users).size());            
            for(int i=0;i<tweetSource.get(users).size();i++)
                System.out.println(tweetSource.get(users).get(i)+"\t:\t"+sourceCounter.get(users).get(i));
        }
    }
    
    
    public int getPostToolNumberSusRate(){
        HashMap<String, Integer> postToolAndNumber = new HashMap<>();
        int suspiciusRate=0;
        int counterOther=0;
        int counterWhole=0;
        if(tweetSource.isEmpty()){

        }else{           
            for(int i=0; i<tweetSource.get(users).size(); i++) {
                if(tweetSource.get(users).get(i).contains("Twitter Web Client")){
                    postToolAndNumber.put("Web", sourceCounter.get(users).get(i));
                    counterWhole= counterWhole + sourceCounter.get(users).get(i);
                }else if(tweetSource.get(users).get(i).contains("Facebook")){
                    postToolAndNumber.put("FacebookApp", sourceCounter.get(users).get(i));
                    counterWhole= counterWhole + sourceCounter.get(users).get(i);
                }else if(tweetSource.get(users).get(i).contains("Twitter for")){                    
                    postToolAndNumber.put("TwitterApp", sourceCounter.get(users).get(i));
                    counterWhole= counterWhole + sourceCounter.get(users).get(i);
                }else{
                    counterWhole= counterWhole + sourceCounter.get(users).get(i);
                    counterOther=counterOther +sourceCounter.get(users).get(i);
                    postToolAndNumber.put("Other", counterOther);  
                }
            }
        }
        for(String n : postToolAndNumber.keySet()){
            if(n.equals("Web")){
                if(counterWhole/postToolAndNumber.get(n)<0.20 && counterWhole>50){
                    suspiciusRate=suspiciusRate+2;        
                }
            }
            //if(n.equals("FacebookApp")){
                //if(counterWhole/postToolAndNumber.get(n)>0.03 && counterWhole>50){
                    //suspiciusRate++;        
                //}
            //}
            if(n.equals("TwitterApp")){
                if(counterWhole/postToolAndNumber.get(n)>0.25 && counterWhole>50){
                    suspiciusRate=suspiciusRate+2;        
                }
            }
            if(n.equals("Other")){
                if(counterWhole/counterOther > 0.20 && counterWhole>50){
                    suspiciusRate=suspiciusRate+2; 
                }
            }
        }
        return suspiciusRate;
    }
    /*
    public int printTweetSourcesForUser(String userID){
        int index=this.userID.indexOf(userID);
        if(index==-1)
            return -1;
        System.out.println("");
        System.out.println("Sources Found: ");
        for(int i=0;i<tweetSource.get(index).size();i++)
            System.out.println(tweetSource.get(index).get(i)+"\t:\t"+sourceCounter.get(index).get(i));
        return 1;
    }   
    */
}
