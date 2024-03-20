# R libraries loading
library(stringi)
library(readr)
library(RSQLite)
library(DBI)
library(lubridate)
library(dplyr)
library(chron)
library(tidyr)
library(gridExtra)
library(ggplot2)

#na
# DB connection
db_file_path <- "script/ecommerce.db"
my_db <- RSQLite::dbConnect(RSQLite::SQLite(),db_file_path)

# Create Tables in DB

# Customer
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS customer(
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
CREATE TABLE IF NOT EXISTS category (
  category_id INT PRIMARY KEY,
  category_name VARCHAR (50) NOT NULL,
  category_description VARCHAR (50) NOT NULL
);
")

# Product
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS product (
  product_id VARCHAR(50) PRIMARY KEY,
  category_id INT,
  product_name VARCHAR(50),
  product_description TEXT,
  registration_date DATE,
  price FLOAT,
  brand VARCHAR(50),
  FOREIGN KEY (category_id) REFERENCES category(category_id)
);
")


# Promotion
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS promotion (
  promotion_id VARCHAR(50) PRIMARY KEY,
  category_id INT,
  promotion_price FLOAT,
  promotion_description TEXT,
  promo_expiration_date DATE,
  FOREIGN KEY (category_id) REFERENCES category(category_id)
);
")

# Seller
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS seller (
    seller_id INT PRIMARY KEY,
    seller_name VARCHAR(100) NOT NULL,
    contact INT NOT NULL,
    email VARCHAR(255) NOT NULL
    );
")

# Provide
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS provide (
    provide_id VARCHAR(50) PRIMARY KEY,
    seller_id VARCHAR(50) REFERENCES seller(seller_id),
    product_id VARCHAR(50) REFERENCES product(product_id)
    );
")

# Order
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    order_date DATE,
    quantity INT,
    product_id VARCHAR(50),
    customer_id INT,
    FOREIGN KEY (product_id) REFERENCES product(product_id), 
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);
")

# Shipment
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS shipping(
    shipping_id INT PRIMARY KEY,
    billing_id INT,
    shipment_status VARCHAR(50),
    FOREIGN KEY (billing_id) REFERENCES transaction_billing(billing_id)
);
")

# Review
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS review (
  review_id VARCHAR(50) PRIMARY KEY,
  customer_id INT,
  product_id VARCHAR(50),
  review_score INT(5),
  FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
  FOREIGN KEY (product_id) REFERENCES product(product_id)
);
")

# Address
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS address (
  address_id VARCHAR(50) PRIMARY KEY,
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
CREATE TABLE IF NOT EXISTS subcategory (
    sub_category_id VARCHAR(50) PRIMARY KEY,
    sub_category_name VARCHAR(50),
    category_id VARCHAR(50),
    FOREIGN KEY (category_id) REFERENCES category(category_id)
);
")

# Transaction billing
dbExecute(my_db, "
CREATE TABLE IF NOT EXISTS transaction_billing (
  billing_id INT PRIMARY KEY,
  order_id INT,
  FOREIGN KEY (order_id) REFERENCES orders(order_id)
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
    # Check if the dataframe is not empty
    if (!is.null(df) && nrow(df) > 0) {
      # Merge the dataframe with the existing merged dataframe
      if (is.null(merged_df)) {
        merged_df <- df
      } else {
        merged_df <- bind_rows(merged_df, df)
      }
    }
  }
  # Assign the merged dataframe to a variable with the entity name
  if (!is.null(merged_df)) {
    assign(paste0("ecom_", entity_name), merged_df, envir = .GlobalEnv)
  }
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
# Define all validation functions

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
  valid.phone.seller <- grepl("^\\d{12}$", phone)
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
  return(!is.na(catname))
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

# Define the pattern for promotion IDs
pattern <- "[A-Z]{2}\\d{2}"

# Apply validation functions to update data frame with valid data and remove invalid data

# Customer data
ecom_customer_data <- ecom_customer_data[sapply(ecom_customer_data$email, check_email_format), ]
ecom_customer_data <- ecom_customer_data[sapply(ecom_customer_data$contact_number, check_phone_format), ]

# Category data
ecom_category_data <- ecom_category_data[sapply(ecom_category_data$category_name, check_names_not_null.s), ]

# Promotion data
ecom_promotion_data <- ecom_promotion_data[grepl(pattern, ecom_promotion_data$promotion_id), ]

filter_promotion_data <- function(promotion_data) {
  # Define the pattern for promotion ID validation
  pattern <- "[A-Z]{2}\\d{2}"
  
  # Filter promotion data based on promotion ID pattern
  promotion_data <- promotion_data[grepl(pattern, promotion_data$promotion_id), ]
  
  # Filter promotion data based on discount validation
  promotion_data <- promotion_data[promotion_data$promotion_price >= 0.1 & promotion_data$promotion_price <= 1.0, ]
  
  return(promotion_data)
}

# Usage:
ecom_promotion_data <- filter_promotion_data(ecom_promotion_data)

# Seller data
ecom_seller_data <- ecom_seller_data[sapply(ecom_seller_data$email, check_email_format), ]

# Review data
revscore.validate.review <- ecom_review$review_score >= 1 & ecom_review$review_score <= 5
ecom_review <- ecom_review[revscore.validate.review, ]
 
# Order data
# ecom_order_data <- ecom_order_data[date_validity.order, ]
# ecom_order_data <- ecom_order_data[quantity.validate.ord, ]
# ecom_order_data <- ecom_order_data[prodname_validity.ord, ]

# Apply validation function to product names to create proname_validity.prod
proname_validity.prod <- sapply(ecom_product_data$product_name, check_names_not_null.s)

# Filter ecom_product_data based on proname_validity.prod
ecom_product_data <- ecom_product_data[proname_validity.prod, ]
# Filter ecom_product_data based on product name length validity
ecom_product_data <- ecom_product_data[sapply(ecom_product_data$product_name, nchar) < 50, ]

# Filter ecom_product_data based on product description length validity
ecom_product_data <- ecom_product_data[sapply(ecom_product_data$product_description, nchar) < 100, ]

# Filter ecom_product_data based on brand name validity
ecom_product_data <- ecom_product_data[sapply(ecom_product_data$brand, check_names_not_null.s), ]

# Filter ecom_product_data based on product price validity
ecom_product_data <- ecom_product_data[ecom_product_data$price <= 10000, ]

# Filter ecom_product_data based on registration date format validity
ecom_product_data <- ecom_product_data[sapply(ecom_product_data$registration_date, check_date_format), ]


# Define valid shipment statuses
valid_statuses <- c("In-Transit", "Shipped", "Delivered", "Out for Delivery")

# Function to check if a shipment status is valid
check_valid_status <- function(status) {
  return(status %in% valid_statuses)
}

# Apply the function to the shipment_status column to create invalid_statuses
invalid_statuses <- !sapply(ecom_shipping$shipment_status, check_valid_status)

# Filter ecom_shipping to remove rows with invalid statuses
ecom_shipping <- ecom_shipping[!invalid_statuses, ]

# Function to check validity of subcategory names
check_subcategory_name_validity <- function(names_vector) {
  valid_names <- sapply(names_vector, check_names_not_null.s)
  return(valid_names)
}

# Apply the function to the subcategory names to create a logical vector indicating validity
valid_subcategory_names <- check_subcategory_name_validity(ecom_sub_category_data$sub_category_name)

# Filter ecom_sub_category_data based on valid subcategory names
ecom_sub_category_data <- ecom_sub_category_data[valid_subcategory_names, ]

# Function to check validity of postal codes
check_postal_code_validity <- function(postal_codes_vector) {
  valid_postal_codes <- sapply(postal_codes_vector, function(postal_code) {
    grepl("^.{3} .{3}$", postal_code)  # Assuming postal codes have the format "XXX XXX"
  })
  return(valid_postal_codes)
}

# Apply the function to the postal codes to create a logical vector indicating validity
valid_postal_codes <- check_postal_code_validity(ecom_address_data$postal_code)

# Filter ecom_address_data based on valid postal codes
ecom_address_data <- ecom_address_data[valid_postal_codes, ]



# Append Data into Database

# db connection
my_db <- RSQLite::dbConnect(RSQLite::SQLite(), db_file_path)

# Function to check if data exists in a table
data_exists <- function(connection, table_name, data_frame) {
  # Construct the query to check for existence of data
  query <- paste0("SELECT COUNT(*) FROM ", table_name)
  result <- dbGetQuery(connection, query)
  return(result[[1]] > 0)
}

# Function to insert data into a table if it doesn't exist
insert_data_if_not_exists <- function(connection, table_name, data_frame) {
  # Check if data already exists in the table
  if (data_exists(connection, table_name)) {
    cat("Data already exists in", table_name, "\n")
    return()
  }
  
  # Extract column names
  columns <- names(data_frame)
  
  # Construct the INSERT INTO SQL query
  insert_query <- paste0("INSERT INTO '", table_name, "' (", paste0("'", columns, "'", collapse = ", "), ") VALUES ")
  
  # Loop through each row of the data frame and insert values
  for (i in 1:nrow(data_frame)) {
    values <- paste0("(", paste0("'", gsub("'", "''", unlist(data_frame[i,])), "'", collapse = ","), ")")
    dbExecute(connection, paste0(insert_query, values))
  }
  
  cat("Data inserted into", table_name, "\n")
}






# Insert data from data frames into respective tables
insert_data_if_not_exists(my_db, "address", ecom_address_data)
insert_data_if_not_exists(my_db, "product", ecom_product_data)
insert_data_if_not_exists(my_db, "seller", ecom_seller_data)
insert_data_if_not_exists(my_db, "category", ecom_category_data)
insert_data_if_not_exists(my_db, "promotion", ecom_promotion_data)
insert_data_if_not_exists(my_db, "shipping", ecom_shipping)
insert_data_if_not_exists(my_db, "customer", ecom_customer_data)
insert_data_if_not_exists(my_db, "provide", ecom_provide_data)
insert_data_if_not_exists(my_db, "subcategory", ecom_sub_category_data)
insert_data_if_not_exists(my_db, "orders", ecom_order_data)
insert_data_if_not_exists(my_db, "review", ecom_review)
insert_data_if_not_exists(my_db, "transaction_billing", ecom_transaction_billing_data)







# Data Analysis 

# Ensure figures directory exists
if (!dir.exists("figures")) {
  dir.create("figures")
}

# PART 1 - DATA ANALYSIS

# Function to generate filename with date-time suffix
generate_filename <- function(prefix) {
  today_date <- as.character(Sys.Date())
  current_time <- format(Sys.time(), format = "%H_%M_%S")
  filename <- paste0("figures/", prefix, "_", today_date, "_", current_time, ".csv")
  return(filename)
}

# 1. Rank order value from highest to lowest
result1 <- dbGetQuery(my_db, "
SELECT 
    o.order_id,
    o.customer_id,
    SUM(o.quantity * p.price) AS total_value
FROM 
    orders o
JOIN 
    product p ON o.product_id = p.product_id
GROUP BY 
    o.order_id, o.customer_id
ORDER BY 
    total_value DESC
LIMIT 10
")
write.csv(result1, generate_filename("rank_order_value"), row.names = FALSE)

# 2. Identify Customers with Most Orders
result2 <- dbGetQuery(my_db, "
SELECT 
    c.customer_id,
    c.first_name, 
    c.last_name, 
    COUNT(*) AS number_of_orders
FROM 
    orders o
JOIN 
    customer c ON o.customer_id = c.customer_id
GROUP BY 
    c.customer_id
ORDER BY 
    number_of_orders DESC
LIMIT 5
")
write.csv(result2, generate_filename("customers_most_orders"), row.names = FALSE)

# 3. Identify the Most Profitable Products
result3 <- dbGetQuery(my_db, "
SELECT 
    p.product_name, 
    (o.quantity * p.price) AS total_amount_sold
FROM 
    orders o
JOIN
    product p ON o.product_id = p.product_id
GROUP BY 
    p.product_name
ORDER BY 
    total_amount_sold DESC
LIMIT 5
")
write.csv(result3, generate_filename("most_profitable_products"), row.names = FALSE)

# 4. Identify Products with the Highest Review Ratings
result4 <- dbGetQuery(my_db, "
SELECT 
    p.product_id,
    p.product_name, 
    AVG(r.review_score) AS avg_review_rating
FROM 
    review r
JOIN 
    product p ON r.product_id = p.product_id
GROUP BY 
    p.product_name
ORDER BY 
    avg_review_rating DESC
LIMIT 5
")
write.csv(result4, generate_filename("products_highest_reviews"), row.names = FALSE)

# 5. Product Sales Rank
result5 <- dbGetQuery(my_db, "
SELECT 
    p.product_id,
    p.product_name,
    SUM(o.quantity) AS quantity_sold
FROM 
    orders o
JOIN 
    product p ON o.product_id = p.product_id
GROUP BY 
    p.product_id, p.product_name
ORDER BY 
    quantity_sold DESC
")
write.csv(result5, generate_filename("product_sales_rank"), row.names = FALSE)

# 6. Category-wise Sales Analysis
result6 <- dbGetQuery(my_db, "
SELECT 
    c.category_id,
    c.category_name,
    COUNT(o.quantity) AS total_sold_unit
FROM 
    orders o
JOIN 
    Product p ON o.product_id = p.product_id
JOIN 
    category c ON p.category_id = c.category_id
GROUP BY 
    c.category_id, c.category_name
ORDER BY 
    total_sold_unit DESC
")
write.csv(result6, generate_filename("category_wise_sales"), row.names = FALSE)

# PART 2 - DATA VISUALIZATION

# SQL Queries
sales_query <- " SELECT 
                  customer.customer_id, city, country, quantity, 
                  shipping.shipment_status, price, brand, subcategory.sub_category_name, 
                  review_score, category.category_name, product_name, product.product_id, 
                  promotion_price, order_date
                FROM customer
                INNER JOIN address ON customer.customer_id = address.customer_id
                INNER JOIN orders ON customer.customer_id = orders.customer_id
                LEFT JOIN product ON product.product_id = orders.product_id
                LEFT JOIN category ON category.category_id = product.category_id
                LEFT JOIN transaction_billing ON orders.order_id = transaction_billing.order_id
                LEFT JOIN shipping ON shipping.billing_id = transaction_billing.billing_id
                LEFT JOIN subcategory ON subcategory.category_id = category.category_id
                LEFT JOIN review ON review.customer_id = customer.customer_id
                LEFT JOIN promotion ON promotion.category_id = category.category_id"


seller_query <- " SELECT 
                    category_name, product_name, seller.seller_id, review_score
                  FROM product
                  LEFT JOIN provide
                  ON product.product_id = provide.product_id
                  LEFT JOIN seller
                  ON provide.seller_id = seller.seller_id
                  LEFT JOIN category
                  ON product.category_id = category.category_id
                  LEFT JOIN review
                  ON product.product_id = review.product_id"

# Data Extraction
sales_analysis <- dbGetQuery(my_db, sales_query)
seller <- dbGetQuery(my_db, seller_query)

# Sales Data Generation
sales_data <- sales_analysis %>% mutate(sales_amount = price * quantity * ifelse(is.na(promotion_price), 1, promotion_price)) 

# Sales Trend by Category
# The average amount of sales by each category is 75,443.85 pounds. Amongst 10 product categories, the sales amount of automotive, 
# beauty_cosmetics, home & garden, pet_supplies and furniture are over the average sales amount by categories. 
category_sales <- sales_data %>% group_by(category_name) %>% summarise(sales_amount = sum(sales_amount))

viz1 <- ggplot(category_sales, aes(x = sales_amount, 
                                   y = reorder(category_name, sales_amount), 
                                   fill = sales_amount)) + 
  geom_col() + 
  geom_vline(aes(xintercept = mean(category_sales$sales_amount), color = "mean"), linetype = "dashed") +
  scale_color_manual(name = " ", values = c(mean = "red")) +
  labs(title = "Sales amount by Category", x = "Sales amount", y = "Category") +
  theme_classic()

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/sales_trend_by_category_", this_filename_date, "_", this_filename_time, ".png"), plot = viz1, device = "png", width = 10, height = 7)

# Sales per unit by category
# This table provides insights into the total units of products sold for each sub-category, organized under their respective parent categories. 
# Understanding sales performance at both sub-category and parent category levels is crucial for businesses to identify top-performing product groups, 
# optimize marketing strategies, and allocate resources effectively across different product categories.

# SQL Queries
top_query <- "SELECT 
                cat.category_id AS parent_category_id,
                cat.category_name AS parent_category_name,
                sub.sub_category_id,
                sub.sub_category_name,
                COUNT(ord.quantity) AS total_sold_units
              FROM orders ord
              JOIN product prod ON ord.product_id = prod.product_id
              JOIN subcategory sub ON prod.category_id = sub.category_id
              JOIN category cat ON sub.category_id = cat.category_id
              GROUP BY cat.category_id, cat.category_name
              ORDER BY total_sold_units DESC"

# Data Extraction
top_categ <- dbGetQuery(my_db, top_query)

viz2 <- ggplot(top_categ, aes(x = parent_category_name, y = total_sold_units, fill = parent_category_name)) +
  geom_bar(stat = "identity") +
  labs(x = "category", y = "Total Sold Units", title = "Total Sales by category") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/sales_unit_by_category_", this_filename_date, "_", this_filename_time, ".png"), plot = viz2, device = "png", width = 10, height = 7)


# Geographical Sales 
# For the analysis of sales by regions, we used three scales. "Over the average" represents the amount of sales greater than the average sales amount by city. 
# "Slightly below the average" and "below the average" represent the amount of sales 1 and 2 standard deviation far from the average, respectively. 
# Geographically, Reading, Portsmouth, Leicester, and Brighton in England had the sales amount over the average sales by city, which is 44,378.74 pounds. 
# There was no city whose sales amount is below the average, but except for four cities having the sales over the average, the rest of the cities in England have the sales amount slightly below the average. 
# Interestingly, the sales of the cities in Scotland show two extreme results, where the sales of Glasgow is greater than average, and those of Edinburgh is below the average. 
# Wales which is Cardiff had the sales over the average. Unsurprisingly, England positions in the first place in total sales amount by country, following by Wales and Scotland.
stats_sales_city <- sales_data %>% 
  group_by(city) %>% 
  summarise(sales_amount = sum(sales_amount)) %>% 
  summarise(mean = mean(sales_amount), sd = sd(sales_amount))

viz3 <- sales_data %>% group_by(country, city) %>% summarise(sales_amount = sum(sales_amount)) %>% 
  mutate(color_condition = case_when(sales_amount > stats_sales_city$mean ~ "1. Over the average",
                                     sales_amount > (stats_sales_city$mean - stats_sales_city$sd) ~ "2. Slightly below the average", 
                                     sales_amount > (stats_sales_city$mean - 2 * stats_sales_city$sd) ~ "3. Below the average")) %>%
  ggplot(aes(x = country, y = city, fill = color_condition)) +
  geom_tile() +
  scale_fill_manual(values = c("1. Over the average" = "steelblue1", "2. Slightly below the average" = "lightcyan3", "3. Below the average" = "coral1")) +
  labs(title = "Geographical Sales Heatmap") +
  theme_classic()

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/geographical_sales_heatmap_", this_filename_date, "_", this_filename_time, ".png"), plot = viz3, device = "png", width = 10, height = 7)

# sales amount by country
viz4 <- sales_data %>% group_by(country) %>% summarise(Total_sales_amount = sum(sales_amount)) %>% 
  ggplot(aes(x = country, y = Total_sales_amount, fill = Total_sales_amount)) + geom_col() + labs(title = "Sales by country") + 
  theme_classic()

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/sales_by_country_", this_filename_date, "_", this_filename_time, ".png"), plot = viz4, device = "png", width = 10, height = 7)

# Sales amount by reviews
viz5 <- sales_data %>%
  group_by(category_name) %>%
  summarise(average_review_score = mean(review_score),
            sales_amount = sum(sales_amount)) %>%
  mutate(color = ifelse(sales_amount > mean(sales_amount),
                        "1. Over the average sales by category",
                        "2. Below the average sales by category")) %>%
  ggplot(aes(x = average_review_score,
             y = reorder(category_name, average_review_score),
             color = color)) +
  geom_point(size = 4) +
  geom_col(aes(fill = color), width = 0.01) +
  scale_fill_manual(values = c("1. Over the average sales by category" = "blue",
                               "2. Below the average sales by category" = "red")) +
  scale_color_manual(values = c("1. Over the average sales by category" = "blue",
                                "2. Below the average sales by category" = "red")) +
  labs(title = "Category by reviews", subtitle = "(Average sales by category = 75,444 pounds)",
       x = "Average review score", y = "Category") +
  theme_classic()

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/category_by_review_", this_filename_date, "_", this_filename_time, ".png"),
       plot = viz5, device = "png", width = 10, height = 7)

# Sales trend by date
# The sales shows the increasing trend over time from 2023 
# to February of 2024. Overall, total sales fluctuates frequently, showing numerous ups and downs. 
# Also, the time-series data appears increasing variances and nearly 4 cycles over time. 
sales_data$order_date <- as.Date(sales_data$order_date)
sales_by_date <- sales_data %>% 
  group_by(order_date, category_name) %>% 
  summarise(sales_amount = sum(sales_amount)) %>% 
  mutate(year = year(order_date), 
         month = month(order_date)) %>% 
  mutate(year_month = sprintf("%04d-%02d", year, month))

# Sales trend
viz6 <- ggplot(sales_by_date, aes(x = order_date, y = sales_amount)) + geom_line() +
  geom_smooth(method = lm, alpha = 0.3, aes(color = "Trend line")) + 
  labs(title = "Sales trend over time", subtitle = "(Average sales amount by date = 13,236 pounds)", x = "Time", y = "Total sales") +
  scale_colour_manual(name=" ", values=c("blue")) +
  geom_hline(aes(yintercept = mean(sales_by_date$sales_amount), linetype = "Average sales by date"), color = "red") + 
  scale_linetype_manual(values = 2) +
  labs(linetype = NULL) +
  theme_classic()

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/sales_trend_over_time_", this_filename_date, "_", this_filename_time, ".png"), plot = viz6, device = "png", width = 10, height = 7)

# Total sales by year
viz7 <-sales_by_date %>% group_by(year_month) %>% ggplot(aes(x = sales_amount, y = year_month)) + 
  geom_col() +
  labs(title = "Total sales by year and month", x = "Total sales", y = "Year and Month") +
  theme_classic()

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/total_sales_by_year_and_month_", this_filename_date, "_", this_filename_time, ".png"), plot = viz7, device = "png", width = 10, height = 7)

# Total sales per category
sales_category_query <- "SELECT 
                          o.order_date, 
                          c.category_id, 
                          c.category_name, 
                          SUM(o.quantity) AS units_sold
                          FROM orders o
                          INNER JOIN product p ON o.product_id = p.product_id
                          INNER JOIN category c ON p.category_id = c.category_id
                          GROUP BY o.order_date, c.category_id, c.category_name
                          ORDER BY o.order_date ASC"

sales_category_data <- dbGetQuery(my_db, sales_category_query)
sales_category_data$order_date <- as.Date(sales_category_data$order_date)

# Check if the data contains at least one non-null value for the faceting variable
if (any(!is.na(sales_category_data$category_name))) {
  # Create the plot only if the faceting variable has at least one non-null value
  viz8 <- ggplot(sales_category_data, aes(x = order_date, y = units_sold, color = category_name)) +
    geom_line() +
    labs(x = "Order Date", y = "Units Sold", title = "Units Sold by Category Across Time") +
    scale_color_discrete(name = "Category") +
    facet_wrap(~ category_name, scales = "free_y", ncol = 2)
  
  # Save the plot with a timestamp
  this_filename_date <- as.character(Sys.Date())
  this_filename_time <- format(Sys.time(), format = "%H_%M")
  ggsave(filename = paste0("figures/unit_sold_by_category_", this_filename_date, "_", this_filename_time, ".png"), 
         plot = viz8, device = "png", width = 10, height = 7)
} else {
  print("Faceting variable has no valid values.")
}

# Price Distribution
price_distribution_query <- "SELECT
                              CASE
                              WHEN price BETWEEN 0 AND 99 THEN '0-99'
                              WHEN price BETWEEN 100 AND 199 THEN '100-199'
                              WHEN price BETWEEN 200 AND 299 THEN '200-299'
                              WHEN price BETWEEN 300 AND 399 THEN '300-399'
                              WHEN price BETWEEN 400 AND 499 THEN '400-499'
                              WHEN price BETWEEN 500 AND 599 THEN '500-599'
                              WHEN price BETWEEN 600 AND 699 THEN '600-699'
                              WHEN price BETWEEN 700 AND 799 THEN '700-799'
                              WHEN price BETWEEN 800 AND 899 THEN '800-899'
                              WHEN price BETWEEN 900 AND 999 THEN '900-999'
                              ELSE '1000+' END AS price_range,
                              COUNT(*) AS product_count
                              FROM product
                              GROUP BY price_range
                              ORDER BY price_range"

price_distribution <- dbGetQuery(my_db, price_distribution_query)

viz9 <- ggplot(price_distribution, aes(x = price_range, y = product_count)) +
  geom_bar(stat = "identity", color = "black", fill = "grey") + # Use a single fill color
  theme_minimal() +
  labs(x = "Price Range ($)", y = "Number of Products", title = "Product Price Distribution") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/price_distribution_", this_filename_date, "_", this_filename_time, ".png"), plot = viz9, device = "png", width = 10, height = 7)


# Customer per City
city_counts_query <- "SELECT 
                        a.city, 
                        COUNT(c.customer_id) AS num_customer_id
                      FROM customer c
                      JOIN address a ON c.customer_id = a.customer_id
                      GROUP BY a.city
                      ORDER BY num_customer_id DESC"

city_counts <- dbGetQuery(my_db, city_counts_query)

viz10 <- ggplot(city_counts, aes(x = reorder(city, -num_customer_id), y = num_customer_id, fill = city)) +
  geom_bar(stat = "identity") +
  labs(x = "City", y = "Number of Customers",
       title = "Number of Customers in Each City") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        legend.position = "none") 

# Save the plot with a timestamp
this_filename_date <- as.character(Sys.Date())
this_filename_time <- format(Sys.time(), format = "%H_%M")
ggsave(filename = paste0("figures/number_of_customer_each_city_", this_filename_date, "_", this_filename_time, ".png"), plot = viz10, device = "png", width = 10, height = 7)

# Disconnect from the database
dbDisconnect(my_db)

