/**
 * @author David Auber 
 * @date 07/10/2016
 * Maître de conférences HDR
 * LaBRI: Université de Bordeaux
 */
package bigdata.hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bigdata.io.Pi;

public class CopyFromLocal extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("HdfsWriter [local input path] [hdfs output path]");
			return 1;
		}

		String localInputPath = args[0];
		URI uri = new URI(args[1]);
		uri = uri.normalize();

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(uri, conf, "auber");
		Path outputPath = new Path(uri.getPath()); 

		if (fs.exists(outputPath)) {
			System.err.println("output path exists");
			return 1;
		}

		OutputStream os = fs.create(outputPath);
		InputStream is = new BufferedInputStream(new FileInputStream(localInputPath));
		IOUtils.copyBytes(is, os, conf);

		os.close();
		is.close();
		return 0;
	}
	
	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new CopyFromLocal(), args));
	}

}
