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

public class MergeFromLocal extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("HdfsWriter [local input paths] [hdfs output path]");
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
		for (int i=1; i < args.length - 1; ++i) {
			InputStream is = new BufferedInputStream(new FileInputStream(args[i]));
			IOUtils.copyBytes(is, os, conf, false);
			is.close();
		}
		os.close();
		return 0;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new MergeFromLocal(), args));
	}

}