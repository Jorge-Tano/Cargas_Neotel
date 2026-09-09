<#
.SYNOPSIS
    Ejecuta una Tarea de Neotel de forma externa via el WebService (ExecuteTask01 o ExecuteTask02).

.DESCRIPTION
    Llama al metodo ExecuteTask01 (1 parametro) o ExecuteTask02 (2 parametros) del WebService de
    Neotel (http://<host>/neoapi/webservice.asmx), segun si se pasa Param2 o no.
    Pensado para la tarea de prueba "TEST ClienteCod (no usar en produccion)" (Id 83), que recibe
    ClienteCod (param1, ej. "0001"=PL, "0289"=REFI) y, si se pasa, TipoOperacion (param2, ej.
    "CREAR", "ACTUALIZAR", "MODIFICAR"), arma internamente Combo = ClienteCod + "-" + TipoOperacion,
    y devuelve un string de resultado segun la rama del flujo que se ejecuto.

.PARAMETER IdTask
    Id numerico de la tarea a ejecutar (columna "Id" en la grilla de Tareas). Por defecto 83
    (la tarea de prueba TEST ClienteCod).

.PARAMETER Param1
    Valor del primer parametro de la tarea (ClienteCod, ej. "0001" o "0289").

.PARAMETER Param2
    Valor del segundo parametro de la tarea (TipoOperacion, ej. "CREAR", "ACTUALIZAR", "MODIFICAR").
    Si no se pasa, se llama a ExecuteTask01 (solo param1). Si se pasa, se llama a ExecuteTask02.

.PARAMETER Username
    Usuario de Neotel. Si no se pasa, se solicita en el momento (no queda en el historial de PowerShell).

.PARAMETER Password
    Password de Neotel, como SecureString. Si no se pasa, se solicita en el momento (oculta al escribir).

.PARAMETER NeotelHost
    IP o nombre del servidor Neotel. Por defecto 192.168.10.17.

.EXAMPLE
    .\Ejecutar-NeotelTask.ps1 -Param1 "0001"
    (llama a ExecuteTask01; usa la tarea de prueba Id 83; pedira usuario y contraseña de forma interactiva)

.EXAMPLE
    .\Ejecutar-NeotelTask.ps1 -Param1 "0001" -Param2 "CREAR"
    (llama a ExecuteTask02, combo esperado "0001-CREAR" -> rama Crear Base PL)

.EXAMPLE
    .\Ejecutar-NeotelTask.ps1 -IdTask 83 -Param1 "0289" -Param2 "MODIFICAR" -Username "mi.usuario"
#>

param(
    [Parameter(Mandatory = $false)]
    [int]$IdTask = 83,

    [Parameter(Mandatory = $true)]
    [string]$Param1,

    [Parameter(Mandatory = $false)]
    [string]$Param2,

    [Parameter(Mandatory = $false)]
    [string]$Username,

    [Parameter(Mandatory = $false)]
    [System.Security.SecureString]$Password,

    [Parameter(Mandatory = $false)]
    [string]$NeotelHost = "192.168.10.17"
)

# --- Pedir credenciales interactivamente si no vinieron por parametro ---
if (-not $Username) {
    $Username = Read-Host "Usuario Neotel"
}
if (-not $Password) {
    $Password = Read-Host "Password Neotel" -AsSecureString
}

$bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($Password)
$plainPassword = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr)
[System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)

# --- Armar el sobre SOAP 1.1 para ExecuteTask01 (idTask + param1) o ExecuteTask02 (idTask + param1 + param2) ---
# Escapamos los valores por si traen caracteres especiales de XML (&, <, >, etc.)
$param1Escaped = [System.Security.SecurityElement]::Escape($Param1)
$usaParam2 = -not [string]::IsNullOrEmpty($Param2)
$metodo = if ($usaParam2) { "ExecuteTask02" } else { "ExecuteTask01" }

if ($usaParam2) {
    $param2Escaped = [System.Security.SecurityElement]::Escape($Param2)
    $paramsXml = "<param1>$param1Escaped</param1>`n      <param2>$param2Escaped</param2>"
}
else {
    $paramsXml = "<param1>$param1Escaped</param1>"
}

$soapEnvelope = @"
<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Header>
    <Authentication xmlns="http://tempuri.org/">
      <Username>$Username</Username>
      <Password>$plainPassword</Password>
    </Authentication>
  </soap:Header>
  <soap:Body>
    <$metodo xmlns="http://tempuri.org/">
      <idTask>$IdTask</idTask>
      $paramsXml
    </$metodo>
  </soap:Body>
</soap:Envelope>
"@

$uri = "http://$NeotelHost/neoapi/webservice.asmx"

try {
    $response = Invoke-WebRequest -Uri $uri -Method Post `
        -ContentType "text/xml; charset=utf-8" `
        -Headers @{ "SOAPAction" = "`"http://tempuri.org/$metodo`"" } `
        -Body $soapEnvelope `
        -UseBasicParsing

    [xml]$xmlResponse = $response.Content
    $result = $xmlResponse.Envelope.Body.($metodo + "Response").($metodo + "Result")

    $paramsTexto = if ($usaParam2) { "param1='$Param1', param2='$Param2'" } else { "param1='$Param1'" }
    Write-Host "Tarea $IdTask ejecutada ($metodo, $paramsTexto) (HTTP $($response.StatusCode))." -ForegroundColor Green

    if ([string]::IsNullOrWhiteSpace($result)) {
        Write-Host "Neotel no devolvio texto en ${metodo}Result. Mostrando la respuesta cruda para diagnostico:" -ForegroundColor Yellow
        Write-Host $response.Content
    }
    else {
        Write-Host "Resultado devuelto por Neotel:" -ForegroundColor Green
        Write-Host $result
    }
}
catch {
    Write-Host "Error al ejecutar la tarea $IdTask contra $uri" -ForegroundColor Red
    Write-Host "Tipo de excepcion: $($_.Exception.GetType().FullName)" -ForegroundColor Red
    Write-Host "Mensaje: $($_.Exception.Message)" -ForegroundColor Red

    # PowerShell 7+ (Invoke-WebRequest lanza HttpResponseException): el cuerpo de error
    # suele venir en $_.ErrorDetails.Message
    if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
        Write-Host "Detalle (ErrorDetails):" -ForegroundColor Yellow
        Write-Host $_.ErrorDetails.Message -ForegroundColor Red
    }

    # Windows PowerShell 5.1 (Invoke-WebRequest lanza WebException): el cuerpo de error
    # viene en el stream de la respuesta, via GetResponseStream()
    if ($_.Exception.Response -and ($_.Exception.Response | Get-Member -Name GetResponseStream -ErrorAction SilentlyContinue)) {
        try {
            $stream = $_.Exception.Response.GetResponseStream()
            $reader = New-Object System.IO.StreamReader($stream)
            $bodyText = $reader.ReadToEnd()
            if (-not [string]::IsNullOrWhiteSpace($bodyText)) {
                Write-Host "Detalle (cuerpo de la respuesta):" -ForegroundColor Yellow
                Write-Host $bodyText -ForegroundColor Red
            }
        }
        catch {
            Write-Host "(No se pudo leer el cuerpo de la respuesta de error: $($_.Exception.Message))" -ForegroundColor DarkYellow
        }
    }
}
finally {
    $plainPassword = $null
}