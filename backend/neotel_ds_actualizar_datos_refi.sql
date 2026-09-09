@BASE VARCHAR(20), @Archivo VARCHAR(500)
AS

DECLARE @_BASE INT
SELECT @_BASE = CONVERT(INT, @BASE)

DECLARE @REGISTROS INT
SELECT @REGISTROS = 0

IF @Archivo IS NOT NULL AND @Archivo <> ''
BEGIN
	CREATE TABLE #TEMP (
		IDINTERNO INT,
		telTelefono2 VARCHAR(60),
		telTelefono3 VARCHAR(60),
		telTelefono1 VARCHAR(60),
		intOrdenDiscado BIGINT,
		intFECHAVCTO BIGINT,
		txtTipoBase VARCHAR(300),
		txtTasa VARCHAR(30),
		txtBDD VARCHAR(300),
		txtFechaCarga VARCHAR(20),
		txtPropension VARCHAR(300),
		txtDCTOTASA VARCHAR(110),
		txtFechainicio VARCHAR(60),
		txtFechafinal VARCHAR(60),
		txtPROPENSIONMORA VARCHAR(160)
	)

	INSERT INTO #TEMP
	EXEC web_callcenter.dbo.OPEN_TXT_FORMAT @Archivo, 'D:\NEOTEL\FTP\UPLOAD\FORMATO INSERT TXT\FORMATO_ACTUALIZAR_REFI.xml'

	IF @@ERROR <> 0
	BEGIN
		SELECT 'ERROR' AS ESTADO, 'Fallo al leer el archivo' AS MENSAJE
		RETURN
	END

	UPDATE A SET
		A.telTelefono2 = B.telTelefono2,
		A.telTelefono3 = B.telTelefono3,
		A.telTelefono1 = B.telTelefono1,
		A.intOrdenDiscado = B.intOrdenDiscado,
		A.intFECHAVCTO = B.intFECHAVCTO,
		A.txtTipoBase = B.txtTipoBase,
		A.txtTasa = B.txtTasa,
		A.txtBDD = B.txtBDD,
		A.txtFechaCarga = B.txtFechaCarga,
		A.txtPropension = B.txtPropension,
		A.txtDCTOTASA = B.txtDCTOTASA,
		A.txtFechainicio = B.txtFechainicio,
		A.txtFechafinal = B.txtFechafinal,
		A.txtPROPENSIONMORA = B.txtPROPENSIONMORA
	FROM CONTACTOS A
	INNER JOIN DB_CONTACTOS D ON A.IDINTERNO = D.IDINTERNO
	INNER JOIN #TEMP B ON A.IDINTERNO = B.IDINTERNO
	WHERE D.IDDATABASE = @_BASE

	SELECT @REGISTROS = @@ROWCOUNT

	EXEC dbo.DB_UPDATE_ORDEN @_BASE, NULL
END

SELECT 'OK' AS ESTADO, 'Registros actualizados: ' + CONVERT(VARCHAR(10), @REGISTROS) AS MENSAJE
