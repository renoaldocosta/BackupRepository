package com.infnet.exemplo;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;

import java.util.Arrays;

public class filterNumbers {
	
    private static JavaSparkContext sc;
    
    //Cria uma função que filtra os números pares
    
    private static Function<Integer, Boolean> filterDiv = (numb -> numb % 2 == 0);

    public static void main(String[] args) {
    	
SparkConf conf = new SparkConf().setAppName("filterNumbers").setMaster("local");
		
		sc = new JavaSparkContext(conf);
		
		// Criando um RDD com uma lista de numeros inteiros
		//Aqui estamos criando uma lista, que no caso é um array com uma única linha
		JavaRDD<Integer> myNumbers = sc.parallelize( Arrays.asList(1,2,3,4,5,6,7,8,9,10) );
		// Executando filtro
		JavaRDD<Integer> filteredRDD = myNumbers.filter(filterDiv);
		// A linha a seguir ira imprimir:  "2 4 6 8 10"
		filteredRDD.foreach(f -> System.out.print( " " + f));
    	
    }
}
