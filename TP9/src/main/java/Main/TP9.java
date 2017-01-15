package Main;

import org.apache.hadoop.util.ProgramDriver;

import partI.KMeans1DMain;
import partII.KMeansNDMain;
//import partIII.KmeansNDHMain;

public class TP9{

	// yarn jar tp9-bigdata-0.0.1.jar ND /cities.txt /res.txt 5 4 5 6


	public static void main(String[] args) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("1D", KMeans1DMain.class, "Partie 1");
			pgd.addClass("ND", KMeansNDMain.class, "Partie 2");
			//pgd.addClass("ND-H", KmeansNDHMain.class, "Partie 3");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}


}
