package com.infnet.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;

import java.util.Arrays;

public class rddFilter {

	private static JavaSparkContext sc;
	
	// Filtrando numeros
	private static Function<Integer, Boolean> filterDiv = (numb -> numb % 2 == 0);

	// Para filtrar apenas numeros maiores que 5, comente o filtro acima e descomente o abaixo:
	//private static Function<Integer, Boolean> filterDiv = (numb -> numb % 2 == 0);

	private static void filterNumbers(){
		// Criando um RDD com uma lista de numeros inteiros
		JavaRDD<Integer> myNumbers = sc.parallelize( Arrays.asList(1,2,3,4,5,6,7,8,9,10) );
		// Executando filtro
		JavaRDD<Integer> filteredRDD = myNumbers.filter(filterDiv);
		// A linha a seguir ira imprimir:  "2 4 6 8 10"
		filteredRDD.foreach(f -> System.out.print(" " + f));
	}

	// Filtrando texto
	private static Function<String, Boolean> filterString = (line -> line.contains("Ipanema") || line.contains("corpo"));
	
	//Neste exemplo desejamos filtrar o texto lido
	//gerando um novo RDD apenas com as linhas que contem
	// uma palavra especifica. Neste caso, "Ipanema" ou "corpo".
	private static void filterText(){

		// Spark HelloWorld! = WordCount
		String filePath = new String("hdfs://quickstart.cloudera/user/cloudera/wordcount/ipanema.txt");
		
		JavaRDD<String> file = sc.textFile(filePath).cache();

		System.out.println("Número de linhas no arquivo: " + file.count());
		
		JavaRDD<String> filteredLines = file.filter(filterString);

		System.out.println("Número de linhas que passaram pelo filtro: " + filteredLines.count());
		
		filteredLines.foreach(line -> System.out.println(">> " + line));
		
		// Quero acessar o dado de indice == index dentro do meu RDD.
		// Neste caso, quero acessar a primeira (index==0) palavra
		// em cada linha do meu RDD filteredLines.
		int index = 0;
		JavaRDD<String> words = filteredLines.map(line -> Arrays.asList(line.split(" ")).get(index));
		
		words.foreach(w -> System.out.println(">> " + w));
		System.out.println(">> " + words.collect());
	}
	
	public static void main(String[] args) {
		
		// Configurando um contexto Spark 
		SparkConf conf = new SparkConf().setAppName("rddFilter").setMaster("local[*]");
		// Instanciando um contexto Spark
		sc = new JavaSparkContext(conf);

		// Exemplo 2: Filtrando dados (inteiros)
		// Neste exemplo iremos criar um RDD como uma 
		// sequencia de numeros inteiros aleatorios.
		filterNumbers();

		// Exemplo 3: Filtrando dados (Strings)
		// Neste exemplo iremos criar um RDD como o
		// conteudo de um arquvio.
		// Instanciando um contexto Spark
		filterText();
		
		sc.close();
	}
}
