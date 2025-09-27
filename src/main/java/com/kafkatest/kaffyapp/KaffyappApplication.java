package com.kafkatest.kaffyapp;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KaffyappApplication {

	@Autowired
	private AdminClient adminClient;


	public static void main(String[] args) {
		SpringApplication.run(KaffyappApplication.class, args);
	}
	@PreDestroy
	public void close (){
		if(adminClient != null){
			adminClient.close();
		}

	}

}
