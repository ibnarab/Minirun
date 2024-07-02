package fonctions

import org.apache.spark.sql.SparkSession

object constants {

        val spark =

          SparkSession.builder

          .appName("multisim")

          .config("spark.hadoop.fs.defaultFS", "hdfs://bigdata")

          .config("spark.master", "yarn")

          .config("spark.submit.deployMode", "cluster")

          .enableHiveSupport()

          .getOrCreate()


        val base_lib_handset                     =    "trusted_cdr.lib_handset" // Partition 2024
        val base_port_dump                       =    "trusted_cdr.port_dump"
        val base_lib_partner_network             =    "trusted_cdr.lib_partner_network" // Prendre tout
        val base_pretups_c2s                     =    "trusted_cdr.pretups_c2s_o2c_c2c"
        val base_ota_compatible_4g               =    "trusted_cem.ota" // Prendre tout
        val base_anonym                          =    "analytics.sampleNBANBO_PREPAID_10" // 1.287.741


        val port_dump_minirun                    =    "trusted_cdr.port_dump_minirun"
        val port_dump_pretups_c2s                =    "trusted_cdr.pretups_c2s_minirun"


}
