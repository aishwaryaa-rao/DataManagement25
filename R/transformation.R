library(RSQLite)
library(readr)

# Reading the csv file
customer_data <- readr::read_csv("data_upload/MOCK_DATA.csv")

# Write the data to db
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(), "database/customer.db")

RSQLite::dbWriteTable(my_connection, "customers", customer_data)
RSQLite::dbDisconnect()
