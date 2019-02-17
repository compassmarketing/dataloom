package com.dataloom

package object er {

  final val DL_ID = "DL_Id"
  final val DL_CLIQUE_ID = "DL_CliqueId"
  final val DL_FNAME = "DL_FirstName"
  final val DL_LNAME = "DL_LastName"
  final val DL_GENDER = "DL_Gender"

  final val REQUIRED_ATTRIBUTES = Array(
    DL_FNAME,
    DL_LNAME,
    DL_GENDER
  )

  final val FEATURE_ATTRIBUTES = REQUIRED_ATTRIBUTES ++ Array(
    "^DL_Street1$",
    "^DL_Street2$",
    "^DL_PostalCode$",
    "^DL_Title$",
    "^DL_Firm$"
  )

  final val UNIQUE_ATTRIBUTES = Array(
    "^DL_Email\\d+$",
    "^DL_Phone\\d+$",
    "^DL_MD5Email\\d+$",
    "^DL_SHA1Email\\d+$",
    "^DL_SHA2Email\\d+$",
    "^DL_IP\\d+$"
  )

  final val ALLOWED_ATTRIBUTES = FEATURE_ATTRIBUTES ++ UNIQUE_ATTRIBUTES

  final val STOP_WORDS = Array(
    "ST",
    "COUNTY",
    "ROAD",
    "PO",
    "BOX",
    "RD",
    "DR",
    "BLVD",
    "CIR",
    "LN",
    "AVE"
  )

}
