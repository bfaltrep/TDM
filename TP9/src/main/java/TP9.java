
import org.apache.hadoop.util.ToolRunner;

public class TP9{

/*
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
	}*/


	//args : inputfile outputdirectory nb_node column_asked
	public static void main(String[] args) throws Exception {
		int length = args.length;
		if(length != 4)
			System.out.println("arguments expected : inputPath outputPath nbNodes column_asked");

		String args_all[] = new String[length+3];
		for (int i = 0; i < args.length; i++) {
			args_all[i] = args[i];
		}
		
		

		int nb_iteration = 0;
		args_all[length] = String.valueOf(nb_iteration); 
		
		//Create two random file names that we will switch at every loop
		int rand = (int)(Math.random()*100);
		args_all[length+2] = "/tmp/"+"kmeans"+"-p"+nb_iteration;
		args_all[length+1] = "/tmp/"+"kmeans"+"-p"+(++nb_iteration);
		
		
		ToolRunner.run(new Kmeans1D(), args_all); //TMP
		
		/*
		while(ToolRunner.run(new Kmeans1D(), args_all) == 0){
			++nb_iteration;
			args_all[length] = String.valueOf(nb_iteration);
			//Switching file paths
			String tmp = args_all[length+1];
			args_all[length+1] = args_all[length+2];
			args_all[length+2] = tmp;
		}*/
		
		//ToolRunner.run(new Kmeans1DFinal(), args); TODOUX
		
		System.exit(0); 
		
	}
}
