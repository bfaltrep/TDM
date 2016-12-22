
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

	private static void removeFromPath(String d1){
		try {
			FileSystem fs = FileSystem.get(new URI(d1), new Configuration(), "bfaltrep");
			fs.delete(new Path((new URI(d1)).getPath()), true);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
	
	
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
		
		//Name two repertories that we will switch at every loop. begin by delete existants.
		args_all[length+2] = "/tmp/"+"kmeans"+"-p"+nb_iteration;
		args_all[length+1] = "/tmp/"+"kmeans"+"-p"+(++nb_iteration);
		removeFromPath(args_all[length+1]); 
		removeFromPath(args_all[length+2]);

		
		if(ToolRunner.run(new Kmeans1D(), args_all) == 0){
			System.out.println("IT WORKED");
		}
		else{
		System.out.println("IT DON T...");
		}
		/*
		while(ToolRunner.run(new Kmeans1D(), args_all) == 0){
			++nb_iteration;
			args_all[length] = String.valueOf(nb_iteration);
			
			//Switching file paths
			String tmp = args_all[length+1];
			args_all[length+1] = args_all[length+2];
			args_all[length+2] = tmp;
			
			removeFromPath(args_all[length+2]);
		}*/
		
		//ToolRunner.run(new Kmeans1DFinal(), args); TODOUX
		
		System.exit(0); 
		
	}
}
