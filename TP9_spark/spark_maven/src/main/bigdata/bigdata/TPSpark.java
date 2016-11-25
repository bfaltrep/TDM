package bigdata;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;

//spark-submit run-example --master yarn --deploy-mode client --jars /net/ens/BigData/spark-2.0.1/examples/jars/spark-examples_2.11-2.0.1.jar --class org.apache.spark.examples.JavaSparkPi --executor-cores 2 --num-executors 4 --executor-memory 512M SparkPi 100

/*
 spark-submit --master yarn --deploy-mode client --class bigdata.TPSpark --num-executors 4 spark_maven/target/TPSpark-0.0.1.jar
 /user/bfaltrep/cities.txt
 
 spark-submit --master yarn --deploy-mode client --class bigdata.TPSpark --num-executors 4 TPSpark-0.0.1.jar /cities.txt
*/

public class TPSpark {

	public static String msg = "                    -----> RESULTATS:       ";
	
	public static void Ex1(JavaSparkContext context, String path){
		//recup file. Input data
		JavaRDD<String> rdd_cities = context.textFile(path);
		
		int nb_part = rdd_cities.getNumPartitions();
		System.out.println(msg+nb_part);
		
		int nb_executors = context.getConf().getInt("spark.executor.instances",1); 
		rdd_cities.coalesce(nb_executors);
		System.out.println(msg+nb_executors);
	}
	//couleur terminal system.out.println("\033[0m");
	//chaque composant \033[0;34m
	//\033[0m final pour reset la suite
	
	public static void Ex2_3(JavaSparkContext context, String path){

		@SuppressWarnings("unchecked")
		JavaRDD<String> rdd_textfile = context.textFile(path);
		JavaRDD<Tuple2<String,Double>>  rdd_cities = rdd_textfile.map(
				(str) -> new Tuple2<String,Double>(
						str.split(",")[1], 
						Double.parseDouble(str.split(",")[4].matches("")||str.split(",")[4].matches("Population")?"-1":str.split(",")[4])));
		rdd_cities = rdd_cities.filter((Tuple2<String,Double> t) -> { if(t._1.matches("")||t._2 == -1) return false;else return true; });
		JavaDoubleRDD rdd_stats = rdd_cities.mapToDouble((Tuple2<String,Double> t) -> {return new Double(t._2);});
		StatCounter stat = rdd_stats.stats();
		
		// printing
		System.out.println(msg+"Ex 2 : "+rdd_cities.count());
		System.out.println(msg+"Ex 3 : max("+stat.max()+"), min("+stat.min()+"), sum("+stat.sum()+"), count("+stat.count()+"), mean("+stat.mean()+"), variance("+stat.variance()+")");
	}
	
	public static void Ex4(JavaSparkContext context, String path){
		JavaRDD<String> rdd_textfile = context.textFile(path);
		//creer un rdd contenant uniquement les villes valides
		JavaDoubleRDD rdd = rdd_textfile.mapToDouble( (String str) -> {
			String[] token = str.split(",");
			try{
				return Double.parseDouble(token[4]);
			}catch(Exception e){
				return -1;
			}
		}).filter( (x) -> {return x > -1;});
		
		//création de nos clés
		JavaPairRDD<Integer,Double> rdd2 = rdd.keyBy((x) -> (int) Math.floor(Math.log10(x)));
		
		JavaPairRDD<Integer,StatCounter> rdd3 = rdd2.aggregateByKey(new StatCounter(), ((agg, x) -> agg.merge(x)), (agg1, agg2) -> { agg1.merge(agg2); return agg1;}).sortByKey();
		
		
		System.out.println(msg+"  DEBUT");
		//rdd3.foreach((Tuple2<Integer,StatCounter> t) -> {System.out.print(msg+t._1+" - "+t._2.count());});
		Iterator<Tuple2<Integer, StatCounter>> it = rdd3.toLocalIterator();
		while(it.hasNext()){
			Tuple2<Integer, StatCounter> t = it.next();
			System.out.print(msg+Math.pow(10, t._1)+" - "+t._2.count()+"\n");
		}
		System.out.println();
		System.out.println(msg+"  FIN");
		
		/* Version courte :
		 List<Tuple2<Integer,StatCounter>> result = rdd.KeyBy( (x) -> (int) Math.floor(Math.log10(x)).aggregateByKey( new StatCounter(), (agg, x) -> agg.merge(x), (agg1, agg2) -> agg1.merge(agg2)).sortByKey().mapToPair( (x) -> new Tuple2<Integer, StatCounter> ( (in) Math.pow(10,x._1), x._2).collect;

		 for(Tuple2<Integer, StatCounter> stat : result){
		 	StringBuffer tmp = new StringBuffer();
		 	tmp.append(stat._1+"\t");
		 	tmp.append(stat._2.count()+"\t");
		 	//other values
		 	System.out.println(tmp.toString());
		 }
		 	
		 */
	}
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("TP_Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		//Ex1(context, args[0]);
		//Ex2_3(context, args[0]);
		Ex4(context, args[0]);
	}
	
}

