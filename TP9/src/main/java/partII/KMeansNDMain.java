package partII;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ToolRunner;

import partI.KMeans1DFinal;

public class KMeansNDMain {

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

	/*
	 * recup les nb_nodes premières villes du fichier d'entrées et les placent dans un fichier ds HDFS dont on retourne le chemin.
	 */
	private static void initPivots(String input, String output, int nb_node, List<Integer> asked){
		try
		{
			// output file : create
			URI output_uri = new URI(output).normalize();
			FileSystem output_fs = FileSystem.get(output_uri, new Configuration(), "bfaltrep");
			Path output_path = new Path(output_uri.getPath());

			if (output_fs.exists(output_path)) { output_fs.delete(output_path, true); } 
			OutputStream os = output_fs.create(output_path, new Progressable(){public void progress(){}});

			// input file : open
			URI input_uri = new URI(input).normalize();
			FileSystem input_fs = FileSystem.get(input_uri,  new Configuration(), "bfaltrep");
			Path input_path = new Path(input_uri.getPath());

			InputStream is = input_fs.open(input_path);

			//copy
			BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( os, "UTF-8"));
			BufferedReader br = new BufferedReader( new InputStreamReader( is, "UTF-8"));
			br.readLine(); //retire la première ligne qui contient les intitulés de colonnes.

			int current_node = 0;
			double value = 0;
			boolean control=true;

			while (current_node < nb_node) {
				String[] blocs = br.readLine().split(",");
				try{
					for(Integer ask : asked){
						// Si une des colonnes demandées contient une valeur non valide, on refuse la ligne via control.
						if(blocs[ask].matches("")){
							control=false;
							break;
						}else{
							value += Double.parseDouble(blocs[ask]);
						}
					}
					if(control){
						bw.write(value+"\n");
						current_node++;
					}
				}catch(Exception e){ e.printStackTrace();}

				control = true;
				value = 0;
			}

			br.close();
			bw.close();
			input_fs.close();
			output_fs.close();
		}catch (Exception e){e.printStackTrace();}
	}

	private static String getPath(String s){
		String[] blocs = s.split("/");
		//récupération du chemin menant au répertoire contenant ce fichier.
		String res = s.substring(0, (s.length()-(blocs[blocs.length-1].length())));
		return res;	
	}

	/* yarn jar tp9-bigdata-0.0.1.jar /cities.txt /res.txt 5 4 */

	//args : inputfile outputdirectory nb_node column_asked1 column_asked2 etc...
	public static void main(String[] args) throws Exception {

		if(args.length < 4){
			System.out.println("arguments expected : inputPath outputPath nbNodes columns_asked...");
			System.exit(0);
		}

		int nb_iteration = 0;
		int length = args.length;
		int pivot_0 = length;
		int pivot_1 = length+1;

		String output_path = args[1];
		String output_directory =  getPath(output_path);
		String args_treatment[] = new String[length+2];

		//supprime le dossier de sortie, s'il existe.
		removeFromPath(output_path);

		//on copie les arguments passés en paramètre dans notre nouvelle chaine d'argument
		for (int i = 0; i < args.length; i++) {
			args_treatment[i] = args[i];
		}

		//On nomme nos deux fichiers temporaires dans lesquels on va écrire les pivots des itérations i et i+1.
		//On intervertira ces fichiers à chaque itération.
		args_treatment[pivot_0] = output_directory+"tmpND/kmeans-p"+nb_iteration;
		args_treatment[pivot_1] = output_directory+"tmpND/kmeans-p"+(nb_iteration+1);

		//Hdfs ne peut pas écrire dans un fichier existant.
		//On détruit donc les fichiers temporaires d'une précédente instance du programme.
		removeFromPath(args_treatment[pivot_1]); 
		removeFromPath(args_treatment[pivot_0]);

		try{
			List<Integer> columns = new ArrayList<Integer>();
			for (int i = 3; i < args.length; i++) {
				columns.add(Integer.parseInt(args[i]));
			}

			// initialisation d'un fichier de pivot avec les nb_nodes premières villes valides.
			initPivots(args[0], args_treatment[pivot_0]+"/part-r-00000", Integer.parseInt(args[2]),columns);
		}catch(Exception e){e.printStackTrace();}

		System.out.println("\033[0;34m iteration "+nb_iteration+"\033[0m"); //TMP
		while(ToolRunner.run(new KMeansND(), args_treatment) == 0){
			++nb_iteration;
			args_treatment[length] = String.valueOf(nb_iteration);

			//On intervertit les fichiers temporaires de calcul des pivots.
			String tmp = args_treatment[pivot_1];
			args_treatment[pivot_1] = args_treatment[pivot_0];
			args_treatment[pivot_0] = tmp;

			removeFromPath(args_treatment[pivot_1]);
			System.out.println("\033[0;34m iteration "+nb_iteration+"\033[0m"); //TMP
		}

		//préparation des arguments pour final
		String args_final[] = new String[length+2];
		for (int i = 0; i < args.length; i++) {
			args_final[i] = args[i];
		}
		args_final[args_final.length-2] =  output_directory+"tmpND/kmeans-p0";
		args_final[args_final.length-1] =  output_directory+"tmpresND";

		// assure que le répertoire de sortie n'existe pas déjà. On utilise le KMeans1DFinal car le traitement est exactement le même ici.
		removeFromPath(args_final[args_final.length-1]); 
		ToolRunner.run(new KMeans1DFinal(), args_final);


		//Renommage et Suppression des fichiers de sortie de MapRaduce :
		// $(output_directory)/tmpresND/part-r-00000 -> $(output_path)
		// suppression $(output_directory)/tmpresND/_SUCCESS
		renameAndClean(output_directory+"tmpresND/part-r-00000",output_path,output_directory+"tmpresND/_SUCCESS");


		removeFromPath(output_directory+"tmpresND");
		// removeFromPath(output_directory+"tmp");
		System.exit(0); 

	}
}
