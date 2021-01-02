package com.group.kaka.stockinfoconsumer;

import com.group.kaka.stockinfoconsumer.consumer.StockConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class StockinfoConsumerApplication implements CommandLineRunner {

	@Autowired
	StockConsumer stockConsumer;

	public static void main(String[] args) {
		SpringApplication.run(StockinfoConsumerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		log.info("Execute Stock Info Consumer .....");
		stockConsumer.consume();

	}
}
