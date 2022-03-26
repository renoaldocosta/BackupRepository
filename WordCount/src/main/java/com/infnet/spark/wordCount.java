package com.infnet.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import scala.Tuple2;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;


public class wordCount {
	
	//Devemos criar o contexto para dizer ao spark como acessar um cluster
	//Estamos instanciando o contexto
	private static JavaSparkContext sc;
	
	public static void main(String[] args) {
		
		//Precisamos do SparkConf para configurar o SparkContext
		//Dizemos o nome do App e setamos o Master
		//Master é uma URL Spark, Mesos ou Yarn ou ainda String dizendo pra rodar local.
		SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local");
		
		//Estamos cria o contexto
		sc = new JavaSparkContext(conf);
		
		//Estamos definindo o arquivo no HDFS a ser acessado.
		//Usamos a configuração do HDFS e colocamos localhost:9000
		String filePath = new String("hdfs://localhost:9000/user/infnet/renoaldo.txt");
		//Aqui estamos abrindo o arquivo e salvando em um RDD do tipo string chamado file
		JavaRDD<String> file = sc.textFile(filePath);
		
		//Estamos imprimindo o tamanho do arquivo. Ou melhor, quantas linhas tem o arquivo.
		System.out.println("Size of file: " + file.count());
		
		//Aqui estamos criando um RDD do tipo string, proveniente do RDD file. Nós estamos fazendo o
		//split das palavras pelo espaço
		JavaRDD<String> words = file.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		//Estamos executando as etapas de map e reduce.
		//Nesse ponto, em counter temos todas as palavras separadas por tuplas e valores
		JavaPairRDD<String, Integer> counter = words.mapToPair(word -> new Tuple2<>(word,1)).reduceByKey((a,b) -> a+b);
		
		//Estamos imprimindo as 3 primeiras tuplas de (palavras, numero_de_ocorrencias)
		System.out.println("3 first words in the count list: " + counter.take(3));
		//Estamos contando quantas palavras tem
		System.out.println("Number of words in file: " + counter.count());
		
		//Agora ordenando por Valor
		//Lembre-se, o metodo sortByKey não será eficiente quando tivermos um RDD com milhões de entradas
		//Estamos ordenando o nosso counter utilizando sortByKey. Ou seja, estamos ordenando por chave. No caso, a palavra
		//
		JavaPairRDD<Integer, String> counterOrder = counter.mapToPair(w -> new Tuple2<Integer, String>(w._2,w._1)).sortByKey(false);
		counterOrder.foreach(w -> System.out.println(w._1+ " -> "+ w._2));
		
		sc.close();
		
		
	}

}















