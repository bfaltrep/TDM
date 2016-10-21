/**
 * @author David Auber 
 * @date 07/10/2016
 * Maître de conférences HDR
 * LaBRI: Université de Bordeaux
 */

package bigdata.hdfs;

import org.apache.hadoop.util.ProgramDriver;

public class TPHdfs {
	
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("copyFromLocal", bigdata.hdfs.CopyFromLocal.class, "reimplmentation of the copy from local hdfs function");
			pgd.addClass("mergeFromLocal", bigdata.hdfs.MergeFromLocal.class, "merge several files into a hdfs file");
			pgd.addClass("wordGenerator", bigdata.hdfs.GenerateWords.class, "random words generator");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}