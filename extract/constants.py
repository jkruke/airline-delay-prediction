class Constants:
    # TODO: dep_estimated_utc/arr_estimated_utc: better "actual" time
    target_csv_columns = ["flight_iata", "airline_iata", "dep_time_utc", "dep_estimated_utc", "arr_time_utc",
                          "arr_estimated_utc", "dep_iata", "arr_iata", "dep_country_code", "arr_country_code",
                          "domestic", "international", "delayed"]


constants = Constants()
