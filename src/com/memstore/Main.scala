package com.memstore
import java.io.File

object Main extends Application{
  
//    declare[String]("Administrationsvej","kode")
//    //declare[String]("ATCKoderOgTekst","tekst")
//    declare[String]("Beregningsregler","kode")
//    declare[Long]("Dosering","doseringKode")
//    //declare[Long]("Doseringskode","drugid")
//    declare[String]("EmballagetypeKoder","kode")
//    declare[Long]("Enhedspriser","varenummer")
//    declare[Long]("Firma","firmanummer")
//    //declare[Long]("Indholdsstoffer","drugID")
//    declare[Long]("Indikation","indikationskode")
//    //declare[Long]("Indikationskode","drugID")
//    declare[String]("Klausulering","kode")
//    declare[Long]("Laegemiddel","drugid")
//    //declare[Long]("LaegemiddelAdministrationsvejRef","drugId")
//    declare[String]("LaegemiddelformBetegnelser","kode")
//    declare[Long]("Laegemiddelnavn","drugid")
//    declare[String]("Medicintilskud","kode")
//    declare[String]("Opbevaringsbetingelser","kode")
//    declare[Long]("OplysningerOmDosisdispensering","varenummer")
//    declare[Long]("Pakning","varenummer")
//    //declare[Long]("Pakningskombinationer","varenummerOrdineret")
//    declare[Long]("PakningskombinationerUdenPriser","varenummerOrdineret")
//    //declare[...]("Pakningsstoerrelsesenhed","enheder")
//    declare[Long]("Priser","varenummer")
//    declare[Long]("Rekommandationer","varenummer")
//    declare[String]("SpecialeForNBS","kode")
//    //declare[...]("Styrkeenhed","enheder")
//    declare[Long]("Substitution","receptensVarenummer")
//    declare[Long]("SubstitutionAfLaegemidlerUdenFastPris","varenummer")
//    //declare[Long]("Takst","substitutionsgruppenummer") I dont think we should load this one
//    //declare[Long]("Tilskudsintervaller","type + niveau")
//    declare[Long]("TilskudsprisgrupperPakningsniveau","varenummer")
//    //declare[Long]("UdgaaedeNavne","drugid")
//    declare[String]("Udleveringsbestemmelser","kode")
  
  val em = Loader.loadPricelists(new File("/Users/chr/ws-scala/pricelist-scala/data/takst"))
  //val em1 = Loader.loadPricelist(new File("/Users/chr/ws-scala/pricelist-scala/data/takst/20100419gl"), em)

//  private def declare[T](entityName: String, attributeName: String) {
//    EntityDescriptor.add(entityName, attributeName)
//  }
  

}