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

# zip util functions for lambda functions
data "archive_file" "utils_layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../utils/"
  output_path      = "${path.module}/../utils_layer.zip"
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
  layers = [aws_lambda_layer_version.dependancy_layer.arn , aws_lambda_layer_version.utils_layer.arn]
}

# define python dependancies layer
resource "aws_lambda_layer_version" "dependancy_layer" {
  layer_name          = "dependancy_layer"
  compatible_runtimes = [var.python_runtime]
  filename            = data.archive_file.dependancy_layer.output_path
}

# define util functions layer
resource "aws_lambda_layer_version" "utils_layer" {
  layer_name          = "utils_layer"
  compatible_runtimes = [var.python_runtime]
  filename            = data.archive_file.utils_layer.output_path
}
