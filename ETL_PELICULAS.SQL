CREATE OR REPLACE PROCEDURE ETL_PELICULAS AS
BEGIN
    -- Truncar la tabla de destino
    EXECUTE IMMEDIATE 'TRUNCATE TABLE C##DWH.TBL_PELICULAS';
    
    -- Insertar registros desde la tabla de origen a la tabla de destino
    INSERT INTO C##DWH.TBL_PELICULAS(
        ID_CODIGO_PRDUCTO,
        TITULO,
        DURACION_MINUTOS
    )
    SELECT "codigo_pelicula",
           "titulo",
           "duracion_minutos"
    FROM TBL_PELICULAS@DATABASELINK_MYSQL 
    
    COMMIT;
    
    P_ETL_LOG(
        P_NOMBRE_ETL => $$PLSQL_UNIT,
        P_FECHA_HORA_INICIO => sysdate,
        P_ESTATUS => 'S',
        P_ERROR => ''
    );
    
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('No se encontraron datos.');
        -- Agregar al log de errores
        P_ETL_LOG(
            P_NOMBRE_ETL => $$PLSQL_UNIT,
            P_FECHA_HORA_INICIO => sysdate,
            P_ESTATUS => 'F',
            P_ERROR => SQLCODE || ' - ' || SQLERRM
        );
    

END ETL_PELICULAS;

SELECT * FROM TBL_PELICULAS;

BEGIN
ETL_PELICULAS;
END;