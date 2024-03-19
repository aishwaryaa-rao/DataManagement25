# R libraries loading
library(stringi)
library(readr)
library(RSQLite)
library(DBI)
library(lubridate)
library(dplyr)
library(chron)
library(tidyr)
library(ggplot2)

# DB connection
my_db <- RSQLite::dbConnect(RSQLite::SQLite(),db_file_path)

# Create Tables in DB

# Customer
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS customer_table(
  customer_id INT PRIMARY KEY,
  email VARCHAR (100) NOT NULL,
  first_name VARCHAR (100) NOT NULL,
  last_name VARCHAR (100) NOT NULL,
  contact_number INT (11) NOT NULL,
  card_number INT(16)
  );
")

# Category
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS category_table (
  category_id INT PRIMARY KEY NOT NULL,
  category_name VARCHAR (50) NOT NULL,
  sub_category VARCHAR (50) NOT NULL,
  category_description VARCHAR (50) NOT NULL
);
")

# Product
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS product_table (
  product_id VARCHAR(50) PRIMARY KEY NOT NULL,
  category_name VARCHAR(50),
  product_name VARCHAR(50),
  product_description TEXT,
  registration_date DATE,
  price FLOAT,
  brand VARCHAR(50),
  category_description TEXT,
  sub_category_name VARCHAR(50),
  FOREIGN KEY (category_description) REFERENCES category_table(category_description),
  FOREIGN KEY (category_name) REFERENCES category_table(category_name),
  FOREIGN KEY (sub_category_name) REFERENCES subcategory_table(sub_category_name)
);
")

# Promotion
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS promotion_table (
  promotion_id VARCHAR(50) PRIMARY KEY NOT NULL,
  category_id INT,
  promo_price FLOAT,
  promotion_description TEXT,
  expiration_date DATE,
  FOREIGN KEY (category_id) REFERENCES category(category_id)
);
")

# Seller
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS seller_table (
    seller_id INT PRIMARY KEY NOT NULL,
    seller_name VARCHAR(100) NOT NULL,
    contact INT NOT NULL,
    email VARCHAR(255) NOT NULL
    );
")

# Provide
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS provide_table (
    provide_id VARCHAR(50) PRIMARY KEY NOT NULL,
    seller_id VARCHAR(50),
    product_id VARCHAR(50),
    FOREIGN KEY (seller_id) REFERENCES seller(seller_id),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
    );
")

# Orders
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS orders_table (
    order_id VARCHAR(50) PRIMARY KEY,
    order_date DATE,
    quantity INT,
    product_id INT, 
    customer_id INT, 
    FOREIGN KEY (product_id) REFERENCES address(address_id),
    FOREIGN KEY (customer_id) REFERENCES address(address_id)
);
")

# Shipment
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS shipment_table (
    shipment_id INT PRIMARY KEY,
    billing_id INT,
    shipment_status VARCHAR(50),
    FOREIGN KEY (billing_id) REFERENCES transaction_billing(billing_id)
);
")

# Review
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS review_table (
  review_id VARCHAR(50) PRIMARY KEY NOT NULL,
  customer_id INT,
  product_id VARCHAR(50),
  review_rating INT(5),
  FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
  FOREIGN KEY (product_id) REFERENCES product(product_id)
);
")

# Address
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS address_table (
  address_id VARCHAR(50) PRIMARY KEY NOT NULL,
  city VARCHAR (50) NOT NULL,
  country VARCHAR (50) NOT NULL,
  postal_code VARCHAR (50) NOT NULL,
  detailed_address VARCHAR (50) NOT NULL,
  customer_id INT,
  FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);
")

# Sub-category
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS subcategory_table (
    subcategory_id VARCHAR(50) PRIMARY KEY NOT NULL,
    subcategory_name VARCHAR(50),
    category_id VARCHAR(50),
    FOREIGN KEY (category_id) REFERENCES category(category_id)
);
")

# Transaction billing
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS transaction_billing_table (
  billing_id INT PRIMARY KEY NOT NULL,
  order_id INT,
  FOREIGN KEY (order_id) REFERENCES shoppingcart_order(order_id)
);
")

# insert data into dataframes

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

# Data Validation

# Function to check email format
check_email_format <- function(email) {
  valid.email <- grepl("^[A-Za-z0-9._&%+-]+@[A-Za-z0-9.-]+\\.com$", email)
  return(valid.email)
}

# Function to check phone number format
check_phone_format <- function(phone) {
  valid.phone <- grepl("^044-\\d{11}$", phone)
  return(valid.phone)
}

# Function to check phone number format for seller
check_phone_format_seller <- function(phone) {
  valid.phone.seller <- grepl("^044-\\d{11}$", phone)
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
db_file_path <- "script/e-commerce.db"
my_db <- RSQLite::dbConnect(RSQLite::SQLite(), db_file_path)

# Write to customer
if (all(email_validity.customer) & all(phone_validity.customer) & all(card_validity.customer) & all(name_validity.customer)) {
  # Read existing primary keys from the database
  existing_keys_cust <- dbGetQuery(my_db, "SELECT customer_id FROM customer_table")
  
  # Extract primary keys from your dataframe
  new_keys <- ecom_customer_data$customer_id 
  
  # Identify new records by comparing primary keys
  new_records_cust <- ecom_customer_data[!new_keys %in% existing_keys_cust$customer_id, ]
  
  # Insert new records into the database
  dbWriteTable(my_db, "customer", new_records_cust, append = TRUE, row.names = FALSE)
  print("Done")
} else {
  print("Error: Customer validation failed.")
}

# Write to category
if (all(catname_validity.category) &  all(id_is_duplicate_category)) {
  # Read existing primary keys from the database
  existing_keys_cat <- dbGetQuery(my_db, "SELECT customer_id FROM customer_table")
  
  # Extract primary keys from your dataframe
  new_keys <- ecom_customer_data$customer_id 
  
  # Identify new records by comparing primary keys
  new_records_cust <- ecom_customer_data[!new_keys %in% existing_keys_cust$customer_id, ]
  
  # Insert new records into the database
  dbWriteTable(my_db, "customer", new_records_cust, append = TRUE, row.names = FALSE)
  print("Done")
} else {
  print("Error: Category validation failed.")
}

# Data Analysis 
# Load required libraries
library(tidyverse)
library(gridExtra)

# DB connection
my_db <- RSQLite::dbConnect(RSQLite::SQLite(),db_file_path)

# Extract data from DB: sales analysis
sales_analysis <- dbGetQuery(my_db, "
SELECT c.customer_id, a.city, a.country, o.quantity, s.shipment_status, p.price, p.brand, sub.subcategory_name, r.review_score, cat.category_name, p.product_name, p.product_id, promo.promo_price, o.order_date
FROM customer AS c
INNER JOIN address AS a
ON c.customer_id = a.customer_id
INNER JOIN \"order\" AS o
ON c.customer_id = o.customer_id
LEFT JOIN product_table AS p
ON p.product_id = o.product_id
LEFT JOIN category AS cat
ON cat.category_name = p.category_name
LEFT JOIN transaction_billing AS tb
ON o.order_id = tb.order_id
LEFT JOIN shipping as s
ON s.billing_id = tb.billing_id
LEFT JOIN subcategory AS sub
ON sub.category_id = cat.category_id
LEFT JOIN review AS r
ON r.customer_id = c.customer_id
LEFT JOIN promotion AS promo
ON promo.category_id = cat.category_id
")

# Extract data from DB: Seller by product category
seller <- dbGetQuery(my_db, "
SELECT category_name, product_name, seller.seller_id, review_score
FROM product
LEFT JOIN provide
ON product.product_id = provide.product_id
LEFT JOIN seller
ON provide.seller_id = seller.seller_id
LEFT JOIN category
ON product.category_id = category.category_id
LEFT JOIN review
ON product.product_id = review.product_w_category$product_id
")

# Sales_data generation
sales_data <- sales_analysis %>% 
  mutate(sales_amount = price * quantity * ifelse(is.na(promo_price), 1, promo_price)) 

# Analysis: Sales trend by category
category_sales <- sales_data %>% 
  group_by(category_name) %>% 
  summarise(sales_amount = sum(sales_amount))

category_plot <- ggplot(category_sales, aes(x = sales_amount, 
                                            y = reorder(category_name, sales_amount), 
                                            fill = sales_amount)) + 
  geom_col() + 
  geom_vline(aes(xintercept = mean(category_sales$sales_amount), color = "mean"), linetype = "dashed") +
  scale_color_manual(name = " ", values = c(mean = "red")) +
  labs(title = "Sales amount by Category", x = "Sales amount", y = "Category") +
  theme_classic()
category_plot

# Analysis: Geographical sales
stats_sales_city <- sales_data %>% 
  group_by(city) %>% 
  summarise(sales_amount = sum(sales_amount)) %>% 
  summarise(mean = mean(sales_amount), sd = sd(sales_amount))

heatmap_plot <- sales_data %>% 
  group_by(country, city) %>% 
  summarise(sales_amount = sum(sales_amount)) %>% 
  mutate(color_condition = case_when(sales_amount > stats_sales_city$mean ~ "1. Over the average",
                                     sales_amount > (stats_sales_city$mean - stats_sales_city$sd) ~ "2. Slightly below the average", 
                                     sales_amount > (stats_sales_city$mean - 2 * stats_sales_city$sd) ~ "3. Below the average")) %>%
  ggplot(aes(x = country, y = city, fill = color_condition)) +
  geom_tile() +
  scale_fill_manual(values = c("1. Over the average" = "steelblue1", "2. Slightly below the average" = "lightcyan3", "3. Below the average" = "coral1")) +
  labs(title = "Geographical Sales Heatmap") +
  theme_classic()
heatmap_plot

sales_by_country_plot <- sales_data %>% 
  group_by(country) %>% 
  summarise(Total_sales_amount = sum(sales_amount)) %>% 
  ggplot(aes(x = country, y = Total_sales_amount, fill = Total_sales_amount)) + 
  geom_col() + 
  labs(title = "Sales by country") + 
  theme_classic()
sales_by_country_plot

# Analysis: Sales amount by reviews
reviews_plot <- sales_data %>% 
  group_by(category_name) %>% 
  summarise(average_review_score = mean(review_score), sales_amount = sum(sales_amount)) %>% 
  mutate(color = case_when(sales_amount > mean(category_sales$sales_amount) ~ "1. Over the average sales by category", 
                           sales_amount < mean(category_sales$sales_amount) ~ "2. Below the average sales by category")) %>%
  ggplot(aes(x = average_review_score, 
             y = reorder(category_name, average_review_score), 
             color = color, 
             fill = color)) + 
  geom_point(size = 4) +
  geom_col(width = 0.01) + 
  labs(title = "Category by reviews", 
       subtitle = "(Average sales by category = 75,444 pounds)", 
       x = "Average review score", 
       y = "Category") +
  theme_classic()
reviews_plot

# Analysis: Sales trend by date
sales_data$order_date <- as.Date(sales_data$order_date)
sales_by_date <- sales_data %>% 
  group_by(order_date, category_name) %>% 
  summarise(sales_amount = sum(sales_amount)) %>% 
  mutate(year = year(order_date), 
         month = month(order_date)) %>% 
  mutate(year_month = sprintf("%04d-%02d", year, month))

sales_trend_plot <- ggplot(sales_by_date, aes(x = order_date, y = sales_amount)) + 
  geom_line() +
  geom_smooth(method = lm, alpha = 0.3, aes(color = "Trend line")) + 
  labs(title = "Sales trend over time", 
       subtitle = "(Average sales amount by date = 13,236 pounds)", 
       x = "Time", 
       y = "Total sales") +
  scale_colour_manual(name = " ", values = c("blue")) +
  geom_hline(aes(yintercept = mean(sales_by_date$sales_amount), linetype = "Average sales by date"), color = "red") + 
  scale_linetype_manual(values = 2) +
  labs(linetype = NULL) +
  theme_classic()
sales_trend_plot



