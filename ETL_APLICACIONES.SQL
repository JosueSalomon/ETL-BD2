SELECT * FROM TBL_APLICACIONES;
CREATE OR REPLACE PROCEDURE ETL_APLICACIONES AS 
    V_FECHA_INICIO DATE;
    V_FECHA_FIN DATE := TRUNC(SYSDATE); -- Fecha actual
    V_HORA_INICIO_PROCESO DATE := SYSDATE;
BEGIN

    EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_APLICACIONES';

    SELECT MIN("fecha_actualizacion") INTO V_FECHA_INICIO FROM tbl_aplicaciones@DATABASELINK_MYSQL;

    IF V_FECHA_INICIO IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('No hay datos para procesar.');
        RETURN;
    END IF;
    
    WHILE V_FECHA_INICIO <= V_FECHA_FIN LOOP
    
    DELETE FROM C##DWH.TBL_APLICACIONES
        WHERE TRUNC(fecha_actualizacion) = TRUNC(V_FECHA_INICIO);
        
    INSERT INTO C##DWH.TBL_APLICACIONES(
        ID_CODIGO_PRDUCTO,
        VERSIONN,
        FECHA_ACTUALIZACION,
        DESARROLLADORA
    )
    SELECT "codigo_aplicacion",
           "version",
           "fecha_actualizacion",
           "nombre_desarrollador"
    FROM tbl_aplicaciones@DATABASELINK_MYSQL A 
    INNER JOIN tbl_desarrolladores@DATABASELINK_MYSQL B
    ON (A."codigo_desarrollador" = B."codigo_desarrollador")
    WHERE TRUNC("fecha_actualizacion") = TRUNC(V_FECHA_INICIO);
    COMMIT;
        
        -- Incrementar la fecha
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
        P_ETL_LOG(
            P_NOMBRE_ETL => $$PLSQL_UNIT,
            P_FECHA_HORA_INICIO => sysdate,
            P_ESTATUS => 'F',
            P_ERROR => SQLCODE || ' - ' || SQLERRM
        );

END ETL_APLICACIONES;

BEGIN
    ETL_APLICACIONES;
END;
