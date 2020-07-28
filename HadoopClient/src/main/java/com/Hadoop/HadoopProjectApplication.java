package com.Hadoop;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.thymeleaf.spring5.expression.Fields;


@SpringBootApplication
public class HadoopProjectApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context=SpringApplication.run(HadoopProjectApplication.class, args);
		
	}

}
