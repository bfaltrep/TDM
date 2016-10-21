/**
 * @author David Auber 
 * @date 07/10/2016
 * Maître de conférences HDR
 * LaBRI: Université de Bordeaux
 */
 package bigdata;

import org.apache.hadoop.util.ProgramDriver;

public class BigData {
	public static void main( String[] args ) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("hdfs", bigdata.hdfs.TPHdfs.class, "use hdfs API");
			pgd.addClass("worldpop", bigdata.worldpop.TPWorldPopulation.class, "filter/resume worlpopulation file");
			pgd.addClass("IOFormat", bigdata.io.TPInputFormat.class, "Create a random point2D import format");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
