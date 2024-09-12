package org.folio.anonymizer;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class EntryPoint {

  public static void main(String[] args) {
    Database.getInstance();

    LoanShuffler.shuffleLoans();
    UserDataOverwriter.overwriteUserData();
    InventoryShuffler.shuffle();
  }
}
