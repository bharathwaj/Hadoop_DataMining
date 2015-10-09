import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;
import twitter4j.Query; 
import twitter4j.QueryResult; 
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;  

public class TwitterStream {
	public static void main(String[] args) throws Exception{
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setOAuthConsumerKey("4MiUSIOEaJea1N1ObNAXAkWoQ"); 
		cb.setOAuthConsumerSecret("fz35RpXBHsb2j2GaPRIjDrbmtvWaZGwUv9rUifOR1DtOL8UlEy");
		cb.setOAuthAccessToken("233938743-Bn4HMGi7yklA7cyCWSMTRygNfzl7k6xJ5PHtIJQO");
		cb.setOAuthAccessTokenSecret("E32VF3YWjckg9IgHGGuDzVXCXEPhqRiKO2D0PvfctTque");
		Twitter twitter = new TwitterFactory(cb.build()).getInstance();
		boolean isNotFirst = false;  
		while(true)  
		{
			FileWriter fstream = new FileWriter("Twitterdata/Chelsea2.txt",true); 
			@SuppressWarnings("resource")  
			BufferedWriter out = new BufferedWriter(fstream); 
			String text = "";
			for (int page = 1; page <= 1000; page++) {   
				Query query = new Query("#CFC");   
				query.count(1000); 
				QueryResult qr = twitter.search(query);
				List<Status> qrTweets = qr.getTweets();   
				if(qrTweets.size() == 0) break; 
				for(Status t : qrTweets) { 
					if(isNotFirst){
						out.write("\n"+t.getId());
					}else{
						out.write(""+t.getId());
					}
					text = t.getText().replaceAll("\\s+|\t|\n", ""); 
					out.write("\t"+text);  
					isNotFirst = true;
				}
			}
			try{ 
				Thread.sleep(1000*60*15);
			}catch(Exception e) {
				System.out.println(e.getMessage());  
			}
		} 
	}
}
