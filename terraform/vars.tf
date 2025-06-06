variable "extract_lambda" {
  type    = string
  default = "extract_lambda"
}
variable "transform_lambda" {
  type    = string
  default = "transform_lambda"
  
}
variable "load_lambda" {
  type    = string
  default = "load_lambda"
  
}
variable "lambda_functions" {
  type    = list(string)
  default = ["extract_lambda", "transform_lambda"]
}


variable "python_runtime" {
  type    = string
  default = "python3.13"
}

# shared db variables
variable "pg_user" {
  sensitive = true
}

variable "pg_port" {
}

# totesys db variables
variable "pg_host" {}

variable "pg_database" {}

variable "pg_password" {
  sensitive = true
}

# data warehouse variables
variable "dw_host" {}

variable "dw_database" {}

variable "dw_password" {
  sensitive = true
}