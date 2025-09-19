# Test script for CSV upload
$boundary = "----FormBoundary7MA4YWxkTrZu0gW"

# Read file contents
$masterContent = Get-Content "test_master_clean.csv" -Raw
$childContent = Get-Content "test_child_clean.csv" -Raw

# Create multipart body
$body = @"
--$boundary
Content-Disposition: form-data; name="master_file"; filename="test_master_clean.csv"
Content-Type: text/csv

$masterContent

--$boundary
Content-Disposition: form-data; name="child_file"; filename="test_child_clean.csv"
Content-Type: text/csv

$childContent

--$boundary--
"@

# Make the request
try {
    $response = Invoke-WebRequest -Uri "http://127.0.0.1:8000/api/master-child-opportunity-update/upload" -Method POST -ContentType "multipart/form-data; boundary=$boundary" -Body $body
    Write-Host "Upload successful!"
    Write-Host "Response:" $response.Content
} catch {
    Write-Host "Upload failed:" $_.Exception.Message
}