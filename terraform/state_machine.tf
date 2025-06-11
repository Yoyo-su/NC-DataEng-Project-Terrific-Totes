resource "aws_sfn_state_machine" "sfn_state_machine" {
  name     = "fscifa-etl-state-machine"
  role_arn = aws_iam_role.state_machine_role.arn

  definition = jsonencode(
    {
      "Comment" : "A description of my state machine",
      "StartAt" : "Lambda Invoke Extract",
      "States" : {
        "Lambda Invoke Extract" : {
          "Type" : "Task",
          "Resource" : "arn:aws:states:::lambda:invoke",
          "Parameters": {
            "FunctionName": "${aws_lambda_function.extract_lambda.arn}",
            "Payload": {}
          },
          "ResultPath": "$.extractResult",
          "Retry" : [
            {
              "ErrorEquals" : [
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException",
                "Lambda.TooManyRequestsException"
              ],
              "IntervalSeconds" : 1,
              "MaxAttempts" : 3,
              "BackoffRate" : 2,
              "JitterStrategy" : "FULL"
            }
          ],
          "Next" : "Lambda Invoke Transform"
        },
        "Lambda Invoke Transform" : {
          "Type" : "Task",
          "Resource" : "arn:aws:states:::lambda:invoke",
          "Parameters": {
            "FunctionName": "${aws_lambda_function.transform_lambda.arn}",
            "Payload": { result : "success" }
          },
          "ResultPath": "$.transformResult",
          "Retry" : [
            {
              "ErrorEquals" : [
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException",
                "Lambda.TooManyRequestsException"
              ],
              "IntervalSeconds" : 1,
              "MaxAttempts" : 3,
              "BackoffRate" : 2,
              "JitterStrategy" : "FULL"
            }
          ],
            "Next": "Lambda Invoke Load"
          },
          "Lambda Invoke Load": {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
              "FunctionName": "${aws_lambda_function.load_lambda.arn}",
              "Payload": { result : "success" }
            },
            "ResultPath": "$.loadResult",
            # "Catch": [
            #   {
            #     "ErrorEquals": ["States.TaskFailed"],
            #     "Next": "ErrorHandledGracefully"
            #   }
            # ],
            "Retry": [
              {
                "ErrorEquals": [
                  "Lambda.ServiceException",
                  "Lambda.AWSLambdaException",
                  "Lambda.SdkClientException",
                  "Lambda.TooManyRequestsException"
                ],
                "IntervalSeconds": 1,
                "MaxAttempts": 3,
                "BackoffRate": 2,
                "JitterStrategy": "FULL"
              }
            ],
          "End" : true
        },
        # "ErrorHandledGracefully": {
        #   "Type": "Pass",
        #   "Result": "Handled final task failure",
        #   "End": true
        # }
      },
  })

}