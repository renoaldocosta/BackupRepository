package com.infnet.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import scala.Tuple2;

import java.util.Arrays;

public class questao2 {
	
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
	}

}







