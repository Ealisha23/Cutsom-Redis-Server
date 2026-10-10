param(
    [string]$Server = 'localhost',
    [int]$Port = 30007   # the NodePort used by the KIND/Helm setup; use 6379 for a local run
)

$client = New-Object System.Net.Sockets.TcpClient($Server, $Port)
$client.ReceiveTimeout = 5000
$stream = $client.GetStream()
$writer = New-Object System.IO.StreamWriter($stream)
$writer.AutoFlush = $true
$reader = New-Object System.IO.StreamReader($stream)

function Send-Command([string]$name, [string]$resp, [int]$extraLines = 0) {
    $writer.Write($resp)
    $lines = @($reader.ReadLine())
    for ($i = 0; $i -lt $extraLines; $i++) { $lines += $reader.ReadLine() }
    Write-Host ("{0,-5} -> {1}" -f $name, ($lines -join ' | '))
}

try {
    Send-Command 'PING' "*1`r`n`$4`r`nPING`r`n"
    Send-Command 'SET'  "*3`r`n`$3`r`nSET`r`n`$3`r`nfoo`r`n`$3`r`nbar`r`n"
    Send-Command 'GET'  "*2`r`n`$3`r`nGET`r`n`$3`r`nfoo`r`n" 1   # a bulk string is two lines
}
finally {
    $reader.Close()
    $writer.Close()
    $client.Close()
}
