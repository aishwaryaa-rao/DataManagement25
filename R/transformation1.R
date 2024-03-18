# R libraries loading
library(stringi)
library(readr)
library(RSQLite)
library(DBI)
library(lubridate)
library(dplyr)
library(chron)

# DB connection
my_db <- RSQLite::dbConnect(RSQLite::SQLite(),"e-commerce.db")

# Create Tables in DB

# Customer table
RSQLite::dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS customer(
  customer_id INT PRIMARY KEY,
  email VARCHAR (100) NOT NULL,
  first_name VARCHAR (100) NOT NULL,
  last_name VARCHAR (100) NOT NULL,
  contact_number INT (11) NOT NULL,
  card_number INT(16)
);")

# Product table
RSQLite::dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS product (
  product_id VARCHAR(50) PRIMARY KEY NOT NULL,
  category_id INT,
  product_name VARCHAR(50),
  product_description TEXT,
  registration_date DATE,
  price FLOAT,
  brand VARCHAR(50),
  FOREIGN KEY (category_id) REFERENCES category(category_id)
);")

# Promotion table
RSQLite::dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS promotion (
  promotion_id VARCHAR(50) PRIMARY KEY NOT NULL,
  category_id INT,
  promo_price FLOAT,
  promotion_description TEXT,
  expiration_date DATE,
  FOREIGN KEY (category_id) REFERENCES category(category_id)
);")

# Seller table
RSQLite::dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS seller (
    seller_id VARCHAR(50) PRIMARY KEY NOT NULL,
    seller_email VARCHAR(255) NOT NULL,
    seller_contactnumber INT NOT NULL,
    seller_name VARCHAR(100) NOT NULL
);")

# Provide table
RSQLite::dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS provide (
    provide_id VARCHAR(50) PRIMARY KEY NOT NULL,
    seller_id VARCHAR(50),
    product_id VARCHAR(50),
    FOREIGN KEY (seller_id) REFERENCES seller(seller_id),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
);")

# Order table
RSQLite::dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS 'order' (
    order_id VARCHAR(50) PRIMARY KEY,
    order_date DATE,
    quantity INT,
    product_id INT, 
    customer_id INT, 
    FOREIGN KEY (product_id) REFERENCES product(product_id),
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);")

# Shipping table
RSQLite::dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS shipping (
    shippment_id INT PRIMARY KEY,
    billing_id INT,
    shipment_status VARCHAR(50),
    FOREIGN KEY (billing_id) REFERENCES transaction_billing(billing_id)
);")

# Review table
RSQLite::dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS review (
  review_id VARCHAR(50) PRIMARY KEY NOT NULL,
  customer_id INT,
  product_id VARCHAR(50),
  review_rating INT(5),
  FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
  FOREIGN KEY (product_id) REFERENCES product(product_id)
);")

# Address table
RSQLite::dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS address (
  address_id VARCHAR(50) PRIMARY KEY NOT NULL,
  city VARCHAR (50) NOT NULL,
  country VARCHAR (50) NOT NULL,
  postal_code VARCHAR (50) NOT NULL,
  detailed_address VARCHAR (50) NOT NULL,
  customer_id INT,
  FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);")

# Category table
RSQLite::dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS category (
  category_id INT PRIMARY KEY NOT NULL,
  category_name VARCHAR (50) NOT NULL,
  category_description VARCHAR (50) NOT NULL
);")

# Sub-category table
RSQLite::dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS subcategory (
    subcategory_id VARCHAR(50) PRIMARY KEY NOT NULL,
    subcategory_name VARCHAR(50),
    category_id VARCHAR(50),
    FOREIGN KEY (category_id) REFERENCES category(category_id)
);")

# Transaction billing table
RSQLite::dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS transaction_billing (
  billing_id INT PRIMARY KEY NOT NULL,
  order_id INT,
  FOREIGN KEY (order_id) REFERENCES 'order'(order_id)
);")

# insert data into dataframes

# Load necessary libraries
library(dplyr)
library(readr)
library(tidyr)

# Define the path to the data_upload folder
data_upload_path <- "data_upload"

# Get a list of all subdirectories within data_upload
subdirectories <- list.dirs(data_upload_path, full.names = TRUE, recursive = FALSE)

# Iterate through each subdirectory
for (entity_folder in subdirectories) {
  # Extract the entity name from the directory path
  entity_name <- basename(entity_folder)
  # Get a list of all CSV files within the entity folder
  csv_files <- list.files(entity_folder, pattern = "*.csv", full.names = TRUE)
  # Initialize an empty dataframe to store the merged data
  merged_df <- NULL
  # Iterate through each CSV file
  for (csv_file in csv_files) {
    # Read the CSV file into a dataframe
    df <- read_csv(csv_file)
    # Merge the dataframe with the existing merged dataframe
    if (is.null(merged_df)) {
      merged_df <- df
    } else {
      merged_df <- bind_rows(merged_df, df)
    }
  }
  # Assign the merged dataframe to a variable with the entity name
  assign(paste0("ecom_", entity_name), merged_df, envir = .GlobalEnv)
}

# Print the names of the created dataframes
# print(ls(pattern = "ecom_"))

# Load necessary libraries
library(dplyr)
library(readr)
library(tidyr)

# Define the path to the data_upload folder
data_upload_path <- "data_upload"

# Get a list of all subdirectories within data_upload
subdirectories <- list.dirs(data_upload_path, full.names = TRUE, recursive = FALSE)

# Iterate through each subdirectory
for (entity_folder in subdirectories) {
  # Extract the entity name from the directory path
  entity_name <- basename(entity_folder)
  # Get a list of all CSV files within the entity folder
  csv_files <- list.files(entity_folder, pattern = "*.csv", full.names = TRUE)
  # Initialize an empty dataframe to store the merged data
  merged_df <- NULL
  # Iterate through each CSV file
  for (csv_file in csv_files) {
    # Read the CSV file into a dataframe
    df <- read_csv(csv_file)
    # Merge the dataframe with the existing merged dataframe
    if (is.null(merged_df)) {
      merged_df <- df
    } else {
      merged_df <- bind_rows(merged_df, df)
    }
  }
  # Assign the merged dataframe to a variable with the entity name
  assign(paste0("ecom_", entity_name), merged_df, envir = .GlobalEnv)
}

# Print the names of the created dataframes
print(ls(pattern = "ecom_"))

# List all entity folders in the shoes_data directory
entity_folders <- list.files("data_upload", full.names = TRUE)

# Create a list to store data frames for each entity
entity_data <- list()

# Loop over each entity folder
for (folder in entity_folders) {
  # Extract the entity name from the folder path
  entity_name <- basename(folder)
  
  # List all CSV files within the entity folder
  csv_files <- list.files(folder, pattern = "\\.csv$", full.names = TRUE)
  
  # Read each CSV file and store it in a data frame
  entity_df <- lapply(csv_files, read.csv)
  
  # Combine all data frames into a single data frame
  entity_df <- do.call(rbind, entity_df)
  
  # Assign the data frame to the list with the entity name as the key
  entity_data[[paste0("ecom_", entity_name)]] <- entity_df
}

# Now, entity_data contains data frames for each entity with names like "shoe_entity_name"
# You can access each data frame using entity_data$shoe_entity_name

# Data Validation

# load libraries
library(RSQLite)
library(DBI)
library(lubridate)

# Function to check email format
check_email_format <- function(email) {
  valid.email <- grepl("^[A-Za-z0-9._&%+-]+@[A-Za-z0-9.-]+\\.com$", email)
  return(valid.email)
}

# Function to check phone number format
check_phone_format <- function(phone) {
  valid.phone <- grepl("^0\\d{11}$", phone)
  return(valid.phone)
}

# Function to check phone number format for seller
check_phone_format_seller <- function(phone) {
  valid.phone.seller <- grepl("^44\\d{10}$", phone)
  return(valid.phone.seller)
}

# Function to card number format
check_card_format <- function(date){
  valid.date <- grepl("^\\d{16}$", date)
  return(valid.date)
}

# Function to check for null names
check_names_not_null <- function(first_name, last_name) {
  return(!is.na(first_name) && nchar(trimws(first_name)) > 0 &&
           !is.na(last_name) && nchar(trimws(last_name)) > 0)
}

# Function to check for null names
check_names_not_null.s <- function(catname) {
  return(!is.na(catname) && nchar(trimws(catname)) > 0)
}

# Function to check for characters in names
check_names_only_chars <- function(names_vector) {
  valid_names <- grepl("^[A-Za-z]+$", names_vector)
  return(valid_names)
}

# Function to check for duplicate ids
check_id <- function(ids, data) {
  if (length(unique(ids)) != nrow(data)) {
    return(TRUE)  # Return TRUE if duplicate IDs are found
  }
  else {
    return(NULL)
  }
}

# Function to check date
check_date_format <- function(date){
  valid.date <- grepl("^\\d{4}-\\d{2}-\\d{2}$", date)
  return(valid.date)
}

# Function to check value range
check_range <- function(value, min_value, max_value) {
  return(value >= min_value & value <= max_value)
}


# Customer Dataframe
# Email 
email_validity.customer <- sapply(ecom_customer_data$email, check_email_format)

# Phone number validation
phone_validity.customer <- sapply(ecom_customer_data$contact_number, check_phone_format)

# Card number validation
card_validity.customer <- sapply(ecom_customer_data$card_number, check_card_format)

# Name validation
name_validity.customer <- sapply(1:length(ecom_customer_data$first_name), function(i) {
  check_names_not_null(ecom_customer_data$first_name[i], ecom_customer_data$last_name[i])
})

# ID validation
# Check for duplicates in the ID column
id_duplicates <- duplicated(ecom_customer_data$customer_id) | duplicated(ecom_customer_data$customer_id, fromLast = TRUE)
id_is_duplicate <- data.frame(ifelse(id_duplicates, TRUE, FALSE))

# Category Dataframe

# Category name not null
catname_validity.category <- sapply(ecom_category_data$category_name, check_names_not_null.s)

# ID validation
# Check for duplicates in the ID column
id_duplicates_category <- duplicated(ecom_category_data$category_id) | duplicated(ecom_category_data$category_id, fromLast = TRUE)
id_is_duplicate_category <- data.frame(ifelse(id_duplicates_category, TRUE, FALSE))

# Append Data into Database

# db connection
my_db <- RSQLite::dbConnect(RSQLite::SQLite(),"e-commerce.db")

# Write to customer
if (all(email_validity.customer) & all(phone_validity.customer) & all(card_validity.customer) & all(name_validity.customer) & all(id_is_duplicate)) {
  RSQLite::dbWriteTable(my_db,"customer",ecom_customer_data)
} else {
  print("Error: Customer validation failed.")
}

# Write to category
if (all(catname_validity.category) &  all(id_is_duplicate_category)) {
  RSQLite::dbWriteTable(my_db,"category",ecom_category_data)
} else {
  print("Error: Category validation failed.")
}

# Data Analysis 


