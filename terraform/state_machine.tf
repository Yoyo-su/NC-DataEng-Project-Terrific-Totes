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
          "Output" : "{% $states.result.Payload %}",
          "Arguments" : {
            "FunctionName" : aws_lambda_function.extract_lambda.arn,
            "Payload" : "{}"
          },
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
          "Output" : "{% $states.result.Payload %}",
          "Arguments" : {
            "FunctionName" : aws_lambda_function.transform_lambda.arn,
            "Payload" : { result : "success" }
          },
          #   "Catch": [ {
          #         "ErrorEquals": ["States.TaskFailed"],
          #         "Next": "Lambda Invoke load"
          #      } ],
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
          #   "Next": "Lambda Invoke Load"
          # },
          # "Lambda Invoke Load": {
          #   "Type": "Task",
          #   "Resource": "arn:aws:states:::lambda:invoke",
          #   "Output": "{% $states.result.Payload %}",
          #   "Arguments": {
          #     "FunctionName": aws_lambda_function.load_lambda.arn,
          #     "Payload": "{% $states.input %}"
          #   },
          #   "Retry": [
          #     {
          #       "ErrorEquals": [
          #         "Lambda.ServiceException",
          #         "Lambda.AWSLambdaException",
          #         "Lambda.SdkClientException",
          #         "Lambda.TooManyRequestsException"
          #       ],
          #       "IntervalSeconds": 1,
          #       "MaxAttempts": 3,
          #       "BackoffRate": 2,
          #       "JitterStrategy": "FULL"
          #     }
          #   ],
          "End" : true
        }
      },
      "QueryLanguage" : "JSONata"
  })

}