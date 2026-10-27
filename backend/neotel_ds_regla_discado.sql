@Client NUM_CLIENTE, @User NUM_USUARIO, @Impersonated IMPERSONIFICADO, @Profile NUM_PERFIL, @IDDATABASE VARCHAR(20), @REGLA VARCHAR(4000)
AS

/*
  Datasource "Regla Discado" de la Tarea 83 (TipoOperacion = REGLA).
  Cambia la "Regla discado" (DB.EXPRESION_REGLA_DISCADO) de la base, lo
  mismo que se hace a mano en la pantalla de la base en Neotel, ej.
  (ISNULL(intOrdenDiscado,0) = 27). El trigger MODIFICAR_DB de la tabla DB
  se ejecuta igual que al guardar desde la pantalla.

  Mismo SQL para cada ECRM: se crea un Datasource por conexión (ECRM_0265
  SAV, ECRM_0001 PL, ECRM_0289 REFI), igual que los de Crear/Actualizar Base.
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

UPDATE DB
SET EXPRESION_REGLA_DISCADO = @REGLA, TS_REGLA = GETDATE()
WHERE IDDATABASE = @_IDDATABASE

IF @@ERROR <> 0
BEGIN
	SELECT 'ERROR' AS ESTADO, 'Fallo al actualizar la regla' AS MENSAJE
	RETURN
END

SELECT 'OK' AS ESTADO, 'Regla de la base ' + @IDDATABASE + ' actualizada: ' + @REGLA AS MENSAJE
