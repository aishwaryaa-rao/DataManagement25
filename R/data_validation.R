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

# Customer table

# Reading the csv file
customer_data <- readr::read_csv("/cloud/project/script/customer_data.csv")

# Email
email_validity.customer <- sapply(customer_data$email, check_email_format)
cust.email.validity <- data.frame(Email = customer_data$email, Valid = email_validity.customer)
invalid.email <- subset(cust.email.validity, cust.email.validity == FALSE)
print(invalid.email)

# Phone number validation
phone_validity.customer <- sapply(customer_data$contact_number, check_phone_format)
cust.phone.validity <- data.frame(Phone = customer_data$contact_number, Valid = phone_validity.customer)
invalid.phone <- subset(cust.phone.validity, cust.phone.validity == FALSE)
print(invalid.phone)

# Card number validation
card_validity.customer <- sapply(customer_data$card_number, check_card_format)
cust.phone.validity <- data.frame(Card = customer_data$card_number, Valid = card_validity.customer)
invalid.phone <- subset(cust.phone.validity, cust.phone.validity == FALSE)
print(invalid.phone)

# Name validation
name_validity.customer <- sapply(1:length(customer_data$first_name), function(i) {
  check_names_not_null(customer_data$first_name[i], customer_data$last_name[i])
})
cust.name.validity <- data.frame(Name = c(customer_data$first_name, customer_data$last_name), Valid = name_validity.customer)
invalid.name <- subset(cust.name.validity, cust.name.validity == FALSE)
print(invalid.name)

# id

# Check for duplicates in the ID column
id_duplicates <- duplicated(ecom_customer_data$id) | duplicated(ecom_customer_data$ID_column, fromLast = TRUE)

# Convert the logical vector to indicate if each value is a duplicate
id_is_duplicate <- ifelse(id_duplicates, TRUE, FALSE)



if (length(unique(customer_data$customer_id)) != nrow(customer_data)) {
  print("Customer ID is not unique.")
}

# Category table

# Reading the csv file
category_data <- readr::read_csv("/cloud/project/script/category_data.csv")

# Category name not null
catname_validity.category <- sapply(category_data$category_name, check_names_not_null)
catname.validity <- data.frame(Name = category_data$category_name, Valid = catname_validity.category)
invalid.catname <- subset(catname.validity, catname.validity == FALSE)
print(invalid.catname)

# cat_name.category <- sapply(category_data$category_name, check_names_only_chars)
# cat_name.char <- data.frame(Name = category_data$category_name, Valid = cat_name.category)
# invalid.catnamechr <- subset(cat_name.char, cat_name.char == FALSE)
# print(invalid.catnamechr)

# id duplicacy
results_catid <- sapply(split(category_data, category_data$category_id), function(data_subset) {
  check_id(data_subset$category_id, data_subset)
})
invalid.catid <- subset(results, results == FALSE)
print(invalid.catid)

# Address table

# Reading the csv file
address_data <- readr::read_csv("/cloud/project/script/address_data.csv")

# address id validation
add_id.address <- sapply(address_data$address_id, check_id)
add_id.validity <- data.frame(id = address_data$address_id, Valid = add_id.address)
invalid.addid <- subset(add_id.validity, add_id.validity == FALSE)
print(invalid.addid)

# Order table

# Reading the csv file
order_data <- readr::read_csv("/cloud/project/script/order_data.csv")

# id duplicacy check
results_ordid <- sapply(split(order_data, order_data$order_id), function(data_subset) {
  check_id(data_subset$order_id, data_subset)
})
invalid.ordid <- subset(results_ordid, results_ordid == FALSE)
print(invalid.ordid)

# date format
date_validity.order <- sapply(order_data$order_date, check_date_format)
ord.date.validity <- data.frame(Date=order_data$order_date, Valid=date_validity.order)
invalid.date <- subset(ord.date.validity, ord.date.validity==FALSE)
print(invalid.date)

# check quantity
quantity.validate.ord <- order_data[order_data$quantity < 1 | order_data$quantity > 10, ]
print(quantity.validate.ord)

# product id
prodname_validity.ord <- sapply(order_data$product_id, check_names_not_null)
prodname.validity <- data.frame(prod = order_data$product_id, Valid = prodname_validity.ord)
invalid.prodname <- subset(prodname.validity, prodname.validity == FALSE)
print(invalid.prodname)

# Product table

# Reading the csv file
product_data <- readr::read_csv("/cloud/project/script/product_data.csv")

# Product name not null
proname_validity.prod <- sapply(product_data$product_name, check_names_not_null)
prodname.validity <- data.frame(Name = product_data$product_name, Valid = proname_validity.prod)
invalid.prodname.prod <- subset(prodname.validity, prodname.validity == FALSE)
print(invalid.prodname.prod)

# Promotion table

# Reading the csv file
promotion_data <- readr::read_csv("/cloud/project/script/promotion_data.csv")

# check discount code
pattern <- "[A-Z]{2}\\d{2}"
invalid.discount.codes <- promotion_data$promotion_id[sapply(promotion_data$promotion_id, grepl, pattern)]
print(invalid.discount.codes)

# check discount
discount.validate.promotion <- promotion_data[promotion_data$promotion_price < 0.1 | promotion_data$promotion_price > 1.0, ]
print(discount.validate.promotion)

# Review table

# Reading the csv file
review_data <- readr::read_csv("/cloud/project/script/review.csv")

# check score
revscore.validate.review <- review_data[review_data$review_score < 1 | review_data$review_score > 5, ]
print(revscore.validate.review)

# Seller table

# Reading the csv file
data_seller <- readr::read_csv("/cloud/project/script/seller_data.csv")

# Email
email_validity.seller <- sapply(data_seller$email, check_email_format)
seller.email.validity <- data.frame(Email = data_seller$email, Valid = email_validity.seller)
invalid.email.seller <- subset(seller.email.validity, seller.email.validity == FALSE)
print(invalid.email.seller)

# Phone number validation
phone_validity.seller <- sapply(data_seller$contact, check_phone_format_seller)
seller.phone.validity <- data.frame(Phone = data_seller$contact, Valid = phone_validity.seller)
invalid.phone.seller <- subset(seller.phone.validity, seller.phone.validity == FALSE)
print(invalid.phone.seller)

# id
if (length(unique(data_seller$seller_id)) != nrow(data_seller)) {
  print("Seller ID is not unique.")
}

# Shipping table

# Reading the csv file
shipping <- readr::read_csv("/cloud/project/script/shipping.csv")

valid_status <- c("In-Transit", "Shipped", "Delivered", "Out for Delivery")
# Check the pattern for each value in the 'Status' column
invalid_statuses <- shipping$shipment_status[!shipping$shipment_status %in% valid_status]
# Print invalid statuses
print(invalid_statuses)


# sub category table

# Reading the csv file
sub_category_data <- readr::read_csv("/cloud/project/script/sub_category_data.csv")

name_validity.subcat <- sapply(sub_category_data$sub_category_name, check_names_not_null)
subcatname.validity <- data.frame(Name = sub_category_data$sub_category_name, Valid = name_validity.subcat)
invalid.subcatname <- subset(subcatname.validity, subcatname.validity == FALSE)
print(invalid.subcatname)





