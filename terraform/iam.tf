# Role and policy for Extract Lambda to Read/Write S3 Injestion bucket

resource "aws_iam_role" "lambda_role" {
  name = "lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}


resource "aws_iam_policy" "lambda_s3_access_policy" {
  name   = "s3-policy-lambda-access"
  policy = data.aws_iam_policy_document.allow_lambda_access_s3.json
}


data "aws_iam_policy_document" "allow_lambda_access_s3" {
  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [
      aws_s3_bucket.ingestion_bucket.arn
    ]
  }


  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject"
    ]
    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}/*"
    ]
  }


  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject"
    ]
    resources = [
      "${aws_s3_bucket.ready_bucket.arn}/*"
    ]
  }
}


resource "aws_iam_policy_attachment" "lambda_s3_policy" {
  name       = "lambda_s3_policy"
  roles      = [aws_iam_role.lambda_role.name]
  policy_arn = aws_iam_policy.lambda_s3_access_policy.arn
}


data "aws_iam_policy_document" "lambda_logging" {
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]

    resources = ["arn:aws:logs:*:*:*"]
  }
}


resource "aws_iam_policy" "lambda_logging" {
  name        = "LambdaCustomLoggingPolicy"
  description = "Custom policy for Lambda to log to CloudWatch"
  policy      = data.aws_iam_policy_document.lambda_logging.json
}


resource "aws_iam_role_policy_attachment" "lambda_custom_logging" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_logging.arn
}


data "aws_iam_policy_document" "terraform_sns_cloudwatch_permissions" {
  statement {
    actions = [
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:DescribeAlarms",
      "sns:CreateTopic",
      "sns:Subscribe",
      "sns:SetTopicAttributes",
      "sns:Publish",
      "lambda:GetFunctionConfiguration",
      "lambda:ListFunctions"
    ]
    resources = ["*"]
  }
}


resource "aws_iam_policy" "lambda_sns_policy" {
  name   = "lambda_cloudwatch_sns_policy"
  policy = data.aws_iam_policy_document.terraform_sns_cloudwatch_permissions.json
}


resource "aws_iam_policy_attachment" "lambda_sns_policy_attachment" {
  name       = "lambda_cloudwatch_sns_policy_attachment"
  roles      = [aws_iam_role.lambda_role.name]
  policy_arn = aws_iam_policy.lambda_sns_policy.arn
}


data "aws_iam_policy_document" "lambda_s3_code_bucket" {
  statement {
    sid    = "AllowS3GetPutAccess"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
      "s3:PutObjectAcl"
    ]

    resources = [
      "arn:aws:s3:::${aws_s3_bucket.code_bucket.bucket}/*"
    ]
  }
}


resource "aws_iam_policy" "lambda_access_code_bucket_policy" {
  name   = "code_bucket_policy"
  policy = data.aws_iam_policy_document.lambda_s3_code_bucket.json
}


resource "aws_iam_policy_attachment" "lambda_code_bucket_policy_attachment" {
  name       = "lambda_code_bucket_policy_attachment"
  roles      = [aws_iam_role.lambda_role.name]
  policy_arn = aws_iam_policy.lambda_access_code_bucket_policy.arn
}


# Setting up roles and policies necessary for State Machine to invoke lambda functions

resource "aws_iam_role" "state_machine_role" {
  name_prefix        = "role-fscifa-etl-state-machine-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "states.amazonaws.com"
      }
    }]
  })
}


resource "aws_iam_role_policy_attachment" "state_machine_policy_attachment" {
  role       = aws_iam_role.state_machine_role.name
  policy_arn = aws_iam_policy.state_machine_policy.arn
}


resource "aws_iam_policy" "state_machine_policy" {
  name_prefix = "policy-state-machine"
  policy      = data.aws_iam_policy_document.state_machine_role_policy.json
}


data "aws_iam_policy_document" "state_machine_role_policy" {
  statement {
    effect = "Allow"
    actions = [
      "lambda:InvokeFunction"
    ]
    resources = ["${aws_lambda_function.extract_lambda.arn}:*","${aws_lambda_function.extract_lambda.arn}",
    "${aws_lambda_function.transform_lambda.arn}",
    "${aws_lambda_function.transform_lambda.arn}:*", "${aws_lambda_function.load_lambda.arn}:*"]
  }
}

# Setting up roles and policies necessary for CloudWatch EventBridge scheduler to trigger state machine

resource "aws_iam_role" "scheduler_role" {
  name_prefix        = "role-currency-scheduler-"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "scheduler.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

resource "aws_iam_role_policy_attachment" "scheduler_policy_attachment" {
  role       = aws_iam_role.scheduler_role.name
  policy_arn = aws_iam_policy.scheduler_policy.arn
}

resource "aws_iam_policy" "scheduler_policy" {
  name_prefix = "policy-fscifa-etl-scheduler-"
  policy      = data.aws_iam_policy_document.scheduler_role_policy.json
}

data "aws_iam_policy_document" "scheduler_role_policy" {
  statement {
    effect = "Allow"
    actions = [
      "states:StartExecution"
    ]
    resources = ["arn:aws:states:eu-west-2:409139324540:stateMachine:*"]
  }
}

resource "aws_iam_role" "lambda_load_role" {
  name = "lambda_load_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

data "aws_iam_policy_document" "allow_load_lambda_to_read_processed_bucket" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = ["${aws_s3_bucket.ready_bucket.arn}", 
    "${aws_s3_bucket.ready_bucket.arn}/*"]
  }
}

resource "aws_iam_policy" "load_lambda_read_policy" {
  name = "s3-let-lambda-read-bucket-pls"
  policy = data.aws_iam_policy_document.allow_load_lambda_to_read_processed_bucket.json
}

resource "aws_iam_role_policy_attachment" "load_lambda_s3_attachment" {
  role      = aws_iam_role.lambda_load_role.name
  policy_arn = aws_iam_policy.load_lambda_read_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_load_logging" {
  role      = aws_iam_role.lambda_load_role.name
  policy_arn = aws_iam_policy.lambda_sns_policy.arn
}

