package partI;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class KMeans1DMain {

	private static void removeFromPath(String d1){
		try {
			FileSystem fs = FileSystem.get(new URI(d1).normalize(), new Configuration(), "bfaltrep");
			fs.delete(new Path((new URI(d1)).getPath()), true);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}


	private static void renameAndClean(String path_src, String path_dst, String to_delete){
		try {
			URI uri = new URI(path_src).normalize();
			FileSystem fs = FileSystem.get(uri, new Configuration(), "bfaltrep");
			if(fs.exists(new Path(path_dst)))
				fs.delete(new Path((new URI(path_dst)).getPath()),true);
			fs.rename(new Path(path_src), new Path(path_dst));
			fs.delete(new Path((new URI(to_delete)).getPath()),true);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

	}

	private static String getPath(String s){
		String[] blocs = s.split("/");
		//récupération du chemin menant au répertoire contenant ce fichier.
		String res = s.substring(0, (s.length()-(blocs[blocs.length-1].length())));
		return res;	
	}

	/* /cities.txt /res.txt 5 4 */

	//args : inputfile outputdirectory nb_node column_asked1 column_asked2 etc...
	public static void main(String[] args) throws Exception {

		if(args.length != 4){
			System.out.println("arguments expected : inputPath outputPath nbNodes columns_asked...");
			System.exit(0);
		}

		int nb_iteration = 0;
		int length = args.length;
		int pivot_1 = length+1;
		int pivot_0 = length+2;

		String output_path = args[1];
		String output_directory =  getPath(output_path);
		String args_treatment[] = new String[length+3];

		//supprime le dossier de sortie, s'il existe.
		removeFromPath(args[1]);

		//on copie les arguments passés en paramètre dans notre nouvelle chaine d'argument
		for (int i = 0; i < args.length; i++) {
			args_treatment[i] = args[i];
		}

		//on ajoute de nouveaux arguments après la liste transmise
		args_treatment[length] = String.valueOf(nb_iteration); 

		//On nomme nos deux fichiers temporaires dans lesquels on va écrire les pivots des itérations i et i+1.
		//On intervertira ces fichiers à chaque itération.
		args_treatment[pivot_0] = output_directory+"tmp1D/kmeans-p"+nb_iteration;
		args_treatment[pivot_1] = output_directory+"tmp1D/kmeans-p"+(nb_iteration+1);
		removeFromPath(args_treatment[pivot_1]); 
		removeFromPath(args_treatment[pivot_0]);

		while(ToolRunner.run(new KMeans1D(), args_treatment) == 0){
			++nb_iteration;
			args_treatment[length] = String.valueOf(nb_iteration);

			//On intervertit les fichiers temporaires de calcul des pivots.
			String tmp = args_treatment[pivot_1];
			args_treatment[pivot_1] = args_treatment[pivot_0];
			args_treatment[pivot_0] = tmp;

			removeFromPath(args_treatment[pivot_1]);
		}

		//préparation des arguments pour final
		String args_final[] = new String[length+2];
		for (int i = 0; i < args.length; i++) {
			args_final[i] = args[i];
		}
		args_final[args_final.length-2] =  output_directory+"tmp1D/"+"kmeans"+"-p0";
		args_final[args_final.length-1] =  output_directory+"tmpres1D";

		// assure que le répertoire de sortie n'existe pas déjà.
		removeFromPath(args_final[args_final.length-1]); // assure que le répertoire de sortie n'existe pas déjà.
		ToolRunner.run(new KMeans1DFinal(), args_final);


		//Renommage et Suppression des fichiers de sortie de MapRaduce :
		// $(output_directory)/tmpres1D/part-r-00000 -> $(output_path)
		// suppression $(output_directory)/tmpres1D/_SUCCESS
		renameAndClean(output_directory+"tmpres1D/part-r-00000",args[1],output_directory+"tmpres1D/_SUCCESS");


		removeFromPath(output_directory+"tmpres1D");
		// removeFromPath(output_directory+"tmp");
		System.exit(0); 

	}
}
