import fonctions.utils._
import fonctions.constants._
import fonctions.read_write._

object Minirun {

  def main(args: Array[String]): Unit = {

    val debut   = args(0)
    val fin     = args(1)
    val date    = args(2)
    val action  = args(3)


    if (action == "port_dump") {
      /*
      Définir START_DATE sur le premier jour du mois precedent
      START_DATE="20240101"
      Définir END_DATE sur le dernier jour du mois precedent
      END_DATE="20240601"
      */
      val df_port_dump = portDump(base_anonym, base_port_dump, debut, fin)
      writeHive(df_port_dump, true, "gzip", port_dump_minirun)

    }
    else if (action == "pretups_c2s") {
      // DATE="20240501"
      val df_pretups_c2s = pretupsC2s(base_pretups_c2s, date)
      /*println("count = "+df_pretups_c2s.count())
      println(df_pretups_c2s.printSchema())
      df_pretups_c2s.show(10, false)*/
      writeHive(df_pretups_c2s, true, "gzip", port_dump_pretups_c2s)
    }




  }

}
