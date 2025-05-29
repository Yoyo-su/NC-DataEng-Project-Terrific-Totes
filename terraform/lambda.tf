data "archive_file" "lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/extract_lambda.py"
  output_path      = "${path.module}/../function.zip"
  
}


data "archive_file" "dependancy_layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../layer/"
  output_path      = "${path.module}/../dependancy_layer.zip"
}

data "archive_file" "utils_layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../src/utils/"
  output_path      = "${path.module}/../utils_layer.zip"
}



resource "aws_lambda_function" "extract_lambda" {
  filename = data.archive_file.lambda.output_path
  function_name = var.lambda_name
  description = ""
  role = aws_iam_role.lambda_role.arn
  handler = "extract_lambda.lambda_handler"
  runtime = var.python_runtime
  timeout = 10
  layers = [aws_lambda_layer_version.dependancy_layer.arn , aws_lambda_layer_version.utils_layer.arn]
}


resource "aws_lambda_layer_version" "dependancy_layer" {
  layer_name          = "dependancy_layer"
  compatible_runtimes = [var.python_runtime]
  filename            = data.archive_file.dependancy_layer.output_path
}

resource "aws_lambda_layer_version" "utils_layer" {
  layer_name          = "utils_layer"
  compatible_runtimes = [var.python_runtime]
  filename            = data.archive_file.utils_layer.output_path
}