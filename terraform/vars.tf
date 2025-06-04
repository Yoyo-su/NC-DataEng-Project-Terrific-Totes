variable "extract_lambda" {
  type    = string
  default = "extract_lambda_handler"
}
variable "transform_lambda" {
  type    = string
  default = "transform_lambda_handler"
  
}

variable "python_runtime" {
  type    = string
  default = "python3.13"
}


variable "pg_user" {
  sensitive = true
}

variable "pg_password" {
  sensitive = true
}

variable "pg_port" {
}


variable "pg_host" {}


variable "pg_database" {}