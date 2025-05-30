# zip extract lambda function
data "archive_file" "extract_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/extract_lambda.py"
  output_path      = "${path.module}/../function.zip"
  
}

# zip python dependancies for lambda functions
data "archive_file" "dependancy_layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../layer/"
  output_path      = "${path.module}/../dependancy_layer.zip"
}


# define extract lambda function
resource "aws_lambda_function" "extract_lambda" {
  filename = data.archive_file.extract_lambda.output_path
  function_name = var.lambda_name
  description = ""
  role = aws_iam_role.lambda_extract_role.arn
  handler = "extract_lambda.lambda_handler"
  runtime = var.python_runtime
  timeout = 30
  layers = [aws_lambda_layer_version.dependancy_layer.arn]
  environment {
    variables = {
      PG_HOST     = var.pg_host
      PG_PORT     = var.pg_port
      PG_USER     = var.pg_user
      PG_PASSWORD = var.pg_password
      PG_DATABASE = var.pg_database
    }
  }
}

# define python dependancies layer
resource "aws_lambda_layer_version" "dependancy_layer" {
  layer_name          = "dependancy_layer"
  compatible_runtimes = [var.python_runtime]
  filename            = data.archive_file.dependancy_layer.output_path
}


