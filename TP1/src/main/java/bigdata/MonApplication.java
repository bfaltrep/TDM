//=====================================================================
/**
* Squelette minimal d'une application Hadoop
* A exporter dans un jar sans les librairies externes
* A exécuter avec la commande ./hadoop jar NOMDUFICHER.jar ARGUMENTS....
*/
package bigdata;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringBufferInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MonApplication {
	
	private static String helloWorld = "HelloWorld";
	private static String copyFromLocal = "CopyFromLocal";
	private static String mergeFromLocal = "MergeFromLocal";
	private static String random = "Random";

	
	/* Runnables */
	
	public static class Random extends Configured implements Tool {
	    private static int NB_SYLLABLES = 46;

	    private List<String> japaneseSyllables = Arrays.asList(  
	            "a",  "i",  "u",  "e",  "o",
	            "ka", "ki", "ku", "ke", "ko",
	            "sa","shi", "su", "se", "so",
	            "ta","chi","tsu", "te", "to",
	            "na", "ni", "nu", "ne", "no",
	            "ha", "hi", "hu", "he", "ho",
	            "ma", "mi", "mu", "me", "mo",
	            "ya",       "yu",       "yo",
	            "ra", "ri", "ru", "re", "ro",
	            "wa",                   "wo",
	            "n");

	    /***
	     * 
	     * @param max : upper limit to the random number generated
	     * @return a pseudo random int between 0 and max (exclude) 
	     */
	    private int myRandom(int max){
	        int res= (int)(Math.random()*max);
	        return res ;
	    }

	    /**
	     * 
	     * @param nbSyllables : length of the word in syllables  
	     * @return a String containing the pseudo-randomly generated word
	     */
	    public String randomJapaneseWord(int nbSyllables){
	        String res ="";
	        for (int i = 0; i < nbSyllables; i++) {
	            res = res.concat(japaneseSyllables.get(myRandom(NB_SYLLABLES)));
	        }
	        return res;
	    }
		
	    /**
	     * Call and print randomJapaneseWord with each arguments passed
	     */
		public int run(String[] args) throws Exception {
			//Output
			URI uri	= new URI( args[1]);
			uri	= uri.normalize();
			Configuration conf = getConf();
			FileSystem fs = FileSystem.get(uri, conf, args[0]);
			Path outputPath = new Path(uri.getPath()); 
			OutputStream os = fs.create(outputPath);
			//Input
			StringBuilder sentence = new StringBuilder();

			for (int i = 2; i < args.length; i++) {
				sentence.append("\n"+randomJapaneseWord(Integer.parseInt(args[i])));
				
			}
			
			//Transfer
			InputStream is = new BufferedInputStream(new StringBufferInputStream(sentence.toString()));
			IOUtils.copyBytes(is, os, conf, false);
			
			is.close();
			os.close();
			return 0;
		}
	}
	
	public static class MergeFromLocal extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			URI uri	= new URI( args[args.length-1]);
			uri	= uri.normalize();
			Configuration conf = getConf();
			FileSystem fs = FileSystem.get(uri, conf, args[0]);
			Path outputPath = new Path(uri.getPath()); 
			OutputStream os = fs.create(outputPath);
			for(int i = 1 ; i < args.length-1; ++i) 
			{
				InputStream is = new BufferedInputStream(new FileInputStream(args[i]));
				IOUtils.copyBytes(is, os, conf, false);
				is.close();
			}
			os.close();
			return 0;
		}
	}
	
	public static class CopyFromLocal extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			String localInputPath = args[1];
			URI uri = new URI(args[2]);
			uri	= uri.normalize();
			Configuration conf= getConf();
			FileSystem fs = FileSystem.get(uri, conf, args[0]); 
			Path outputPath= new Path(uri.getPath()); 
			OutputStream os	= fs.create(outputPath);
			InputStream	is = new BufferedInputStream(new FileInputStream(localInputPath));
			IOUtils.copyBytes(is, os, conf);
			os.close();
			is.close();
			return 0;
		}
	}
	
	public static class HelloWorld extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			System.out.println("Hello World");
			return 0;
		}
	}
		
	private static void usage(String[] args)
	{
		if(args.length == 0)
		{
			System.out.println("usage : hadoop jar MyApplication <command name> <arg1> <arg2> etc");
			System.out.println("Command name can be : HelloWorld");
			System.out.println("                      CopyFromLocal <my user name> <origin> <dest name>");
			System.out.println("                      MergeFromLocal <my user name> <src file> <src file>... <destination>");
			System.out.println("                      Random <my user name> <destination> <number> <number>...");
			System.exit(-1);
		}
	}
	
	/* Main */
	
	public static void main( String[] args ) throws Exception {
		int returnCode = -1;
		
		usage(args);
		
		List<String> list = new ArrayList<String>(Arrays.asList(args));
		list.remove(0);
		
		String[] array = new String[args.length-1];		
		array = list.toArray(array);
		
		
		if(args[0].matches(helloWorld))
			returnCode = ToolRunner.run(new MonApplication.HelloWorld(), array); 
		else if(args[0].matches(copyFromLocal))
			returnCode = ToolRunner.run(new MonApplication.CopyFromLocal(), array); 
		else if(args[0].matches(mergeFromLocal))
			returnCode = ToolRunner.run(new MonApplication.MergeFromLocal(), array);
		else if(args[0].matches(random))
			returnCode = ToolRunner.run(new MonApplication.Random(), array);
		else
			System.out.println("please choose a treatment : HelloWorld, CopyFromLocal, MergeFromLocal or Random.");
			
		
		System.exit(returnCode);
	}
}
//=====================================================================

