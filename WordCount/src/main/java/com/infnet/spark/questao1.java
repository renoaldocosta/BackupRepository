package com.infnet.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import scala.Tuple2;

import java.util.Arrays;

public class questao1 {
	
	private static JavaSparkContext sc;
	
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("questao1").setMaster("local");
		sc = new JavaSparkContext(conf);
		
		String caminhoDoArquivo = new String("hdfs://localhost:9000/user/infnet/datasets/crimes-2001-to-present.csv");
		JavaRDD<String> crimesData = sc.textFile(caminhoDoArquivo).cache();
		
		String cabecalho = crimesData.first();
		crimesData = crimesData.filter(linha -> !linha.equals(cabecalho)).cache();
	}

}







