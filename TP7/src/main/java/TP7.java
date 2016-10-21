import org.apache.hadoop.util.ProgramDriver;

public class TP7{


	public static void main(String[] args) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			pgd.addClass("ReduceSideJoin", ReduceSideJoin.class, "Partie 1");
			pgd.addClass("ScalableJoin", ScalableJoin.class, "Partie 2");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
}
