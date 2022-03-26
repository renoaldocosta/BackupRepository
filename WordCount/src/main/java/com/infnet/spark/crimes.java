package com.infnet.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;

import scala.Tuple2;

import java.util.Arrays;

public class crimes {
	//criamos o contexto
	private static JavaSparkContext sc;
	
	public static void main(String[] args) {
		//Instanciando um contexto
		SparkConf conf = new SparkConf().setAppName("ElucidarCrimes").setMaster("local");
		sc = new JavaSparkContext(conf);
		
		//Criando um RDD do tipo String
		String filePath = new String("hdfs://localhost:9000/user/infnet/datasets/crimes-2001-to-present.csv");
		JavaRDD<String> dataset = sc.textFile(filePath).cache();
		
		//copiando a primeira linha para uma variavel chamada header
		String header = dataset.first();
		//filtrando o header
		dataset = dataset.filter(line -> !line.equals(header)).cache();
		//Criando um RDD chamado primaryTypeColumn com a 6ª coluna sob índice 5.
		JavaRDD<String> primaryTypeColumn = dataset.map(line -> Arrays.asList(line.split(",")).get(5));
		
		//Executar o map reduce para contar quantos crimeType tem.
		JavaPairRDD<String, Integer> typeCounter = primaryTypeColumn
				.mapToPair(crimeType -> new Tuple2<>(crimeType,1))
				.reduceByKey((a,b) -> a+b);
		//Agora vamos ordenar por valor
		JavaPairRDD<Integer, String> counterOrder = typeCounter
				.mapToPair(w -> new Tuple2<Integer, String>(w._2, w._1))
				.sortByKey(false);
		counterOrder.coalesce(1).foreach(w -> System.out.println("\t" + w._1 + "t" + w._2));
		
		System.out.println(">> Temos ao todo "+ counterOrder.count() + " tipos diferentes de crimes.");
		
		counterOrder.coalesce(1).saveAsTextFile("hdfs://localhost:9000/user/infnet/datasets/crimeOutput");
		sc.stop();
		
	
	}

}








