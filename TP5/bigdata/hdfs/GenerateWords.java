/**
 * @author David Auber 
 * @date 07/10/2016
 * Maître de conférences HDR
 * LaBRI: Université de Bordeaux
 */
package bigdata.hdfs;

import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GenerateWords extends Configured implements Tool {

	String alphabet[] = {"ca","co","ce","la", "le","lu","la","il","el","be","par","pir","por","ou","da","de","di","do","du"};
	
	public int run(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("generateWords [nb syl] [nb words] [URI]");
			return 1;
		}

		URI uri = new URI(args[args.length - 1]);
		uri = uri.normalize();
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(uri, conf, "hadoop");
		Path outputPath = new Path(uri.getPath()); 
		if (fs.exists(outputPath)) {
			System.err.println("output path exists");
			return 1;
		} 
		OutputStream os = fs.create(outputPath);
		PrintStream ps = new PrintStream(os); 
		Random rand = new Random();
		final int nbwords = Integer.parseInt(args[1]);
		final int maxSyl = Integer.parseInt(args[0]);
		
		for ( int i=0; i<nbwords; ++i) {
			StringBuffer tmp = new StringBuffer();
			for (int nbSyllabe=0; nbSyllabe<maxSyl; ++nbSyllabe) {
				tmp.append(alphabet[rand.nextInt(alphabet.length)]);
			}
			tmp.append("\n");
			ps.print(tmp.toString());
		}			
		os.close();
		return 0;
	}
	
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new GenerateWords(), args));
	}
}
