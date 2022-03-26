package com.infnet.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import scala.Tuple2;

import java.util.Arrays;

public class questao3 {
	
	private static JavaSparkContext sc;
	
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("questao1").setMaster("local");
		sc = new JavaSparkContext(conf);
		
		String caminhoDoArquivo = new String("hdfs://localhost:9000/user/infnet/datasets/crimes-2001-to-present.csv");
		JavaRDD<String> crimesData = sc.textFile(caminhoDoArquivo).cache();
		
		System.out.println("O cabeçalho: "+crimesData.take(1));
		
		String cabecalho = crimesData.first();
		crimesData = crimesData.filter(linha -> !linha.equals(cabecalho)).cache();
		
		JavaRDD<String> ColunaLocationDescription = crimesData.map(linha -> Arrays.asList(linha.split(",")).get(7));
		
		System.out.println("5 primeiros tipos de localidade da tabela: "+ ColunaLocationDescription.take(5));
		
		JavaPairRDD<String, Integer> contadorDeLocalidades = ColunaLocationDescription
				.mapToPair(tipoDeLocalidade -> new Tuple2<> (tipoDeLocalidade,1))
				.reduceByKey((a,b) -> a+b);
		
		System.out.println("Quantidade de localidades: " + contadorDeLocalidades.count());
		
		JavaPairRDD<Integer, String> contadorDeLocalidadesOrdenado = contadorDeLocalidades
				.mapToPair(objeto -> new Tuple2<Integer, String>(objeto._2,objeto._1))
				.sortByKey(false);
		contadorDeLocalidadesOrdenado.coalesce(1).foreach(objeto -> System.out.println("\t" + objeto._1 + "\t" + objeto._2));
		
		System.out.println("Temos ao todo " + contadorDeLocalidadesOrdenado.count() + " Lugares diferentes.");
		
		contadorDeLocalidadesOrdenado.coalesce(1).saveAsTextFile("hdfs://localhost:9000/user/infnet/datasets/questao3");
		
		sc.stop();
	}

}







