SELECT * FROM TBL_HISTORIAL_DESCARGAS;
CREATE OR REPLACE PROCEDURE ETL_HISTORIAL_DESCARGAS AS
    V_FECHA_INICIO DATE;
    V_FECHA_FIN DATE := TRUNC(SYSDATE); -- Fecha actual
    V_HORA_INICIO_PROCESO DATE := SYSDATE;
BEGIN

    EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_HISTORIAL_DESCARGAS';

    SELECT MIN("fecha_descarga") INTO V_FECHA_INICIO FROM tbl_historial_descargas@DATABASELINK_MYSQL;

    IF V_FECHA_INICIO IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('No hay datos para procesar.');
        RETURN;
    END IF;

    WHILE V_FECHA_INICIO <= V_FECHA_FIN LOOP

    DELETE FROM C##DWH.TBL_HISTORIAL_DESCARGAS
    WHERE TRUNC(FECHA_DESCARGA) = TRUNC(V_FECHA_INICIO);
    
    INSERT INTO C##DWH.TBL_HISTORIAL_DESCARGAS(
        ID_CODIGO_DESCARGA,
        FECHA_DESCARGA,
        ESTADO,
        ID_USUARIO,
        ID_CODIGO_PRDUCTO
    )
    SELECT A."codigo_descarga", A."fecha_descarga", B."nombre_estatus",
        A."codigo_usuario", A."codigo_producto"
        FROM tbl_historial_descargas@DATABASELINK_MYSQL A
        INNER JOIN tbl_estatus_descarga@DATABASELINK_MYSQL B
        ON (A."codigo_estatus" = B."codigo_estatus")
        WHERE TRUNC("fecha_descarga") = TRUNC(V_FECHA_INICIO);
        
COMMIT;
         V_FECHA_INICIO := V_FECHA_INICIO + 1;
    END LOOP;
      P_ETL_LOG(
        P_NOMBRE_ETL => $$PLSQL_UNIT,
        P_FECHA_HORA_INICIO => sysdate,
        P_ESTATUS => 'S',
        P_ERROR => ''
    );
    -- Hacer commit para confirmar los cambios en la base de datos
    COMMIT;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No se encontraron datos.');
        -- Agregar al log de errores
        ROLLBACK;
        P_ETL_LOG(
            P_NOMBRE_ETL => $$PLSQL_UNIT,
            P_FECHA_HORA_INICIO => sysdate,
            P_ESTATUS => 'F',
            P_ERROR => SQLCODE || ' - ' || SQLERRM
        );

END ETL_HISTORIAL_DESCARGAS;

BEGIN
ETL_HISTORIAL_DESCARGAS;
END;


SELECT A."codigo_descarga", A."fecha_descarga", B."nombre_estatus",
        A."codigo_usuario", A."codigo_producto"
FROM tbl_historial_descargas@DATABASELINK_MYSQL A
INNER JOIN tbl_estatus_descarga@DATABASELINK_MYSQL B
ON (A."codigo_estatus" = B."codigo_estatus");