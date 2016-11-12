import org.apache.hadoop.util.ProgramDriver;

public class TP9{


	public static void main(String[] args) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("Kmeans", Kmeans.class, "Partie 1");
			//pgd.addClass("Kmeans-rec", Kmeans.class, "Partie 1-2");
			//pgd.addClass("words", Kmeans.class, "Partie 1");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
