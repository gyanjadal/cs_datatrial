# Set variables
$FunctionName = "gj-exchange-poller"
$ZipFile = "gj-exchange-poller-lambda_function.zip"
$TsFunctionDir = "..\gj-exchange-poller"  # Directory where Function Code exists

# Clean up
Write-Output "Cleaning up..."
Remove-Item $ZipFile

# Transpile TypeScript to JavaScript
Write-Output "Transpiling TypeScript files..."
npm run build --prefix $TsFunctionDir

# Package Lambda function
Write-Output "Packaging Lambda function..."
Compress-Archive -Path "$TsFunctionDir\dist\*.js", "$TsFunctionDir\node_modules\*" -DestinationPath $ZipFile

# Deploy to AWS Lambda
Write-Output "Deploying Lambda function to AWS..."
aws lambda update-function-code `
  --function-name $FunctionName `
  --zip-file "fileb://$ZipFile" `
  --region us-west-2
# Clean up
#Write-Output "Cleaning up..."
#Remove-Item $ZipFile

Write-Output "Deployment complete!"
