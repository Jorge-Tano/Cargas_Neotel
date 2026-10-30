@Client NUM_CLIENTE, @User NUM_USUARIO, @Impersonated IMPERSONIFICADO, @Profile NUM_PERFIL, @IDDATABASE VARCHAR(20), @REGLA VARCHAR(4000)
AS

/*
  Datasource "Regla Discado" de la Tarea 83 (TipoOperacion = REGLA).
  Hace lo mismo que guardar la "Regla discado" desde la pantalla de la base
  en Neotel (comparado contra la pantalla el 29-10-2026, base SAV 224):

    1. Guarda la regla (DB.EXPRESION_REGLA_DISCADO, TS_REGLA).
    2. CIERRA los contactos abiertos (GESTIONADO = 'N') que NO cumplen la
       regla nueva: GESTIONADO 'S', SUBCATEGORIA/SUBCATEGORIA_1 = 25
       ("OutDialerRule", cerrado por regla de discado), FH_CIERRE = hoy.
       No cierra agendas (IDVENTA_AGENDA <> 0) porque la base tiene
       "Agendas" desactivado en la regla.
    3. REABRE solo los que estaban cerrados por regla (SUBCATEGORIA = 25) y
       ahora SÍ la cumplen: GESTIONADO 'N', SUBCATEGORIA/SUBCATEGORIA_1 = 1
       ("NotProcessed"), FH_CIERRE vacío.

  Lo gestionado por agentes (cualquier otra subcategoría) no se toca.
  Sin el paso 2 y 3 el discador no se entera del cambio (sigue discando lo
  que estaba abierto), que es lo que pasó al cambiar solo el texto.

  Mismo SQL para cada ECRM (un Datasource por conexión).
*/

DECLARE @_IDDATABASE INT
SELECT @_IDDATABASE = TRY_CONVERT(INT, @IDDATABASE)

IF @_IDDATABASE IS NULL OR NOT EXISTS (SELECT * FROM DB WHERE IDDATABASE = @_IDDATABASE)
BEGIN
	SELECT 'ERROR' AS ESTADO, 'No existe la base IDDATABASE=' + ISNULL(@IDDATABASE, '(vacio)') AS MENSAJE
	RETURN
END

IF ISNULL(LTRIM(RTRIM(@REGLA)), '') = ''
BEGIN
	SELECT 'ERROR' AS ESTADO, 'La regla viene vacia' AS MENSAJE
	RETURN
END

DECLARE @CERRADOS INT = 0, @REABIERTOS INT = 0
DECLARE @SQL NVARCHAR(MAX) = N'
	UPDATE D
	SET GESTIONADO = ''S'', SUBCATEGORIA = 25, SUBCATEGORIA_1 = 25, FH_CIERRE = CONVERT(date, GETDATE())
	FROM DB_CONTACTOS D
	JOIN CONTACTOS C ON C.IDINTERNO = D.IDINTERNO
	WHERE D.IDDATABASE = @B AND D.GESTIONADO = ''N'' AND ISNULL(D.IDVENTA_AGENDA, 0) = 0
	  AND NOT (' + @REGLA + N')
	SET @C = @@ROWCOUNT

	UPDATE D
	SET GESTIONADO = ''N'', SUBCATEGORIA = 1, SUBCATEGORIA_1 = 1, FH_CIERRE = NULL
	FROM DB_CONTACTOS D
	JOIN CONTACTOS C ON C.IDINTERNO = D.IDINTERNO
	WHERE D.IDDATABASE = @B AND D.SUBCATEGORIA = 25
	  AND (' + @REGLA + N')
	SET @R = @@ROWCOUNT'

BEGIN TRY
	BEGIN TRAN

	UPDATE DB
	SET EXPRESION_REGLA_DISCADO = @REGLA, TS_REGLA = GETDATE()
	WHERE IDDATABASE = @_IDDATABASE

	EXEC sp_executesql @SQL, N'@B INT, @C INT OUTPUT, @R INT OUTPUT',
		@B = @_IDDATABASE, @C = @CERRADOS OUTPUT, @R = @REABIERTOS OUTPUT

	COMMIT TRAN
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK TRAN
	SELECT 'ERROR' AS ESTADO, 'Fallo al aplicar la regla (no se cambio nada): ' + ERROR_MESSAGE() AS MENSAJE
	RETURN
END CATCH

SELECT 'OK' AS ESTADO,
	'Regla de la base ' + @IDDATABASE + ' actualizada: ' + @REGLA
	+ ' | cerrados por regla: ' + CONVERT(varchar(10), @CERRADOS)
	+ ' | reabiertos: ' + CONVERT(varchar(10), @REABIERTOS) AS MENSAJE
