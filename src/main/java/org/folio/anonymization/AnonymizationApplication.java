package org.folio.anonymization;

import lombok.extern.log4j.Log4j2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.resilience.annotation.EnableResilientMethods;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Log4j2
@SpringBootApplication
@EnableResilientMethods
@EnableTransactionManagement
public class AnonymizationApplication {

  public static void main(String[] args) {
    SpringApplication.run(AnonymizationApplication.class, args);
  }
}
